module.exports = {
  command: 'list [bucket] [prefix]',
  desc: 'list resources in an S3 bucket',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket', {
      type: 'string',
      desc: 'S3 bucket from which to fetch the object',
      alias: 'b'
    })
    .positional('prefix', {
      type: 'string',
      desc: 'prefix of the keys to list',
      alias: 'p'
    })
    .option('delimiter', {
      type: 'string',
      desc: 'the heirarchy delimiter to use for key listings',
      default: '/'
    })
    .option('start-after', {
      type: 'string',
      desc: 'start after this key'
    })
    .option('format', {
      type: 'string',
      desc: 'the format for the listed values',
      choices: ['bucket-key', 'key', 'url'],
      default: 'key',
      alias: 'f'
    })
    .option('limit', {
      type: 'number',
      desc: 'limit the number of keys returned',
      default: 100,
      alias: 'l'
    })
    .option('unlimited', {
      type: 'boolean',
      desc: 'list all keys everything',
      alias: 'u'
    })
}

function handler (options) {
  if (!options.bucket) {
    formatBuckets(listBuckets())
  } else {
    formatKeys(listKeys(options), options)
  }
}

// List S3 buckets
function listBuckets () {
  const aws = require('aws-sdk')
  const moment = require('moment')
  const EventEmitter = require('events')

  const s3 = new aws.S3()
  const events = new EventEmitter()

  const getBucketRegion = async bucket => {
    return new Promise((resolve, reject) => {
      s3.getBucketLocation({ Bucket: bucket }, (error, result) => {
        error ? reject(error) : resolve(result.LocationConstraint || 'us-east-1')
      })
    })
  }

  s3.listBuckets(async (error, result) => {
    if (error) {
      events.emit('error', error)
    } else {
      const {
        Buckets: bucketNames
      } = result

      try {
        const buckets = await Promise.all(bucketNames.map(
          async ({ Name: bucket, CreationDate: created }) => ({
            bucket,
            created: moment(created).utc(),
            region: (await getBucketRegion(bucket))
          })
        ))
        buckets.forEach(bucket => events.emit('bucket', bucket))
        events.emit('end', { count: buckets.length })
      } catch (error) {
        events.emit('error', error)
      }
    }
  })

  return events
}

// List S3 keys
function listKeys ({ bucket, prefix, delimiter, startAfter, limit, unlimited }) {
  const aws = require('aws-sdk')
  const moment = require('moment')
  const EventEmitter = require('events')

  const s3 = new aws.S3()
  const events = new EventEmitter()

  let keyCount = 0
  let prefixCount = 0

  const listMore = token => {
    const options = {
      Bucket: bucket,
      Prefix: prefix,
      Delimiter: delimiter,
      StartAfter: startAfter,
      MaxKeys: Math.min(1000, unlimited ? 1000 : (limit - keyCount))
    }

    if (token) {
      options.ContinuationToken = token
    }

    s3.listObjectsV2(options, (error, result) => {
      if (error) {
        events.emit('error', error)
      } else {
        const {
          Contents: keys,
          IsTruncated: isTruncated,
          NextContinuationToken: token,
          CommonPrefixes: prefixes
        } = result

        prefixes.forEach(({
          Prefix: prefix
        }) => {
          prefixCount++
          const url = `s3://${bucket}/${prefix}`
          events.emit('prefix', {
            bucket,
            delimiter,
            prefix,
            url
          })
        })

        const maxSize = keys.reduce((acc, { Size: size }) => Math.max(size, acc), 0)

        keys.forEach(({
          Key: key,
          LastModified: modified,
          ETag: eTag,
          Size: size,
          StorageClass: storage
        }) => {
          keyCount++
          const url = `s3://${bucket}/${key}`
          events.emit('key', {
            bucket,
            delimiter,
            key,
            modified: moment(modified).utc(),
            maxSize,
            size,
            url
          })
        })

        if (isTruncated && (unlimited || limit > keyCount)) {
          listMore(token)
        } else {
          events.emit('end', {
            prefixCount,
            keyCount,
            more: isTruncated
          })
        }
      }
    })
  }

  listMore()

  return events
}

// Format and print buckets as they are observed
function formatBuckets (buckets) {
  const c = require('@buzuli/color')

  const regionDecor = c.pool()
  const formatBucket = ({ bucket, region, created }) => {
    const dateStr = created.format('YYYY-MM-DD')
    const timeStr = c.grey(created.format('HH:mm:ss'))
    const regionStr = regionDecor(region)
    const bucketStr = c.yellow(bucket)

    return `  [${dateStr} ${timeStr} | ${regionStr}] ${bucketStr}`
  }

  buckets.on('bucket', bucket => console.info(formatBucket(bucket)))

  buckets.once('end', ({ count }) => {
    console.info(`Listed ${c.orange(count)} buckets.`)
  })

  buckets.once('error', error => {
    console.error(error)
    console.error(c.red('Error listing buckets. Details above ☝🏼'))
    process.exit(1)
  })
}

// Format and print keys as they are observed
function formatKeys (keys, { format }) {
  const c = require('@buzuli/color')
  const { padString } = require('../lib/util')

  keys.on('prefix', ({ bucket, delimiter, prefix, url }) => {
    switch (format) {
      case 'bucket-key': return console.info(`  ${bucket} ${prefix}`)
      case 'key': return console.info(`  ${prefix}`)
      case 'url': return console.info(`  ${url}`)
    }
  })

  keys.on('key', ({ bucket, delimiter, key, maxSize, modified, size, url }) => {
    const pad = `${maxSize}`.length
    const dateStr = modified.format('YYYY-MM-DD')
    const timeStr = c.grey(modified.format('HH:mm:ss'))
    const sizeStr = c.orange(padString(pad, `${size}`))
    const decorate = text => `  [${dateStr} ${timeStr} | ${sizeStr}] ${text}`
    switch (format) {
      case 'bucket-key': return console.info(decorate(`${c.blue(bucket)} ${c.yellow(key)}`))
      case 'key': return console.info(decorate(c.yellow(key)))
      case 'url': return console.info(decorate(`${c.green('s3')}://${c.blue(bucket)}/${c.yellow(key)}`))
    }
  })

  keys.once('end', ({ prefixCount, keyCount, more }) => {
    const all = more ? '' : 'all '
    const partial = more ? ' (partial listing)' : ''
    if (prefixCount > 0) {
      console.info(`Listed ${c.orange(prefixCount)} common prefixes.`)
    }
    console.info(`Listed ${all}${c.orange(keyCount)} matching keys${partial}.`)
  })

  keys.once('error', error => {
    console.error(error)
    console.error(c.red('Error listing keys. Details above ☝🏼'))
    process.exit(1)
  })
}
