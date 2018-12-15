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
      alisa: 'b'
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
      type: 'number',
      desc: 'list all keys everything',
      alisa: 'u'
    })
}

function handler (options) {
  const {
    bucket,
    prefix,
    delimiter,
    startAfter,
    format,
    limit,
    unlimited
  } = options

  if (!bucket) {
    formatBuckets(listBuckets())
  } else {
    formatKeys(listKeys({
      bucket,
      prefix,
      delimiter,
      startAfter,
      limit: unlimited ? -1 : limit
    }), { format })
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
        Buckets: bucketNames,
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
      } catch (error) {
        events.emit('error', error)
      }
    }
  })

  return events
}

// List S3 keys
function listKeys ({ bucket, prefix, delimiter, startAfter, limit }) {
  const aws = require('aws-sdk')
  const moment = require('moment')
  const EventEmitter = require('events')

  const s3 = new aws.S3()
  const events = new EventEmitter()

  const total = 0
  const count = 0

  const listMore = token => {
    const options = {
      Bucket: bucket,
      Prefix: prefix,
      Delimiter: delimiter,
      StartAfter: startAfter,
      MaxKeys: Math.min(1000, limit - total)
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
          const url = `s3://${bucket}/${prefix}`
          events.emit('prefix', {
            bucket,
            delimiter,
            prefix,
            url
          })
        })

        keys.forEach(({
          Key: key,
          LastModified: modified,
          ETag: eTag,
          Size: size,
          StorageClass: storage
        }) => {
          const url = `s3://${bucket}/${key}`
          events.emit('key', {
            bucket,
            delimiter,
            key,
            modified: moment(modified).utc(),
            size,
            url
          })
        })

        if (isTruncated && limit > total) {
          listMore(token)
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

  buckets.once('end', ({ more, count, total }) => {
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
      case 'bucket-key': return console.info(`${bucket} ${prefix}`)
      case 'key': return console.info(prefix)
      case 'url': return console.info(url)
    }
  })

  keys.on('key', ({ bucket, delimiter, key, modified, size, url }) => {
    const dateStr = modified.format('YYYY-MM-DD')
    const timeStr = c.grey(modified.format('HH:mm:ss'))
    const sizeStr = c.orange(padString(20, `${size}`))
    const decorate = text => `  [${dateStr} ${timeStr} | ${sizeStr}] ${text}`
    switch (format) {
      case 'bucket-key': return console.info(decorate(`${c.blue(bucket)} ${c.yellow(key)}`))
      case 'key': return console.info(decorate(c.yellow(key)))
      case 'url': return console.info(decorate(c.green(url)))
    }
  })

  keys.once('end', ({ count, more, total }) => {
    const all = more ? '' : ' all'
    const partial = more ? ' (partial listing)' : ''
    console.info(`Listed ${all}${c.orange(count)} keys${partial}.`)
  })

  keys.once('error', error => {
    console.error(error)
    console.error(c.red('Error listing keys. Details above ☝🏼'))
    process.exit(1)
  })
}
