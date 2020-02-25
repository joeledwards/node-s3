const c = require('@buzuli/color')
const fs = require('fs')
const aws = require('aws-sdk')
const moment = require('moment')
const throttle = require('@buzuli/throttle')
const durations = require('durations')
const { resolveResourceInfo } = require('../lib/util')

module.exports = {
  command: 'list-multipart <bucket-or-uri> [prefix]',
  desc: 'list incomplete multi-part uploads',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'the bucket containing the prefix or the uri to scan for multi-part uploads'
    })
    .positional('prefix', {
      type: 'string',
      desc: 'the key prefix to which the scan should be limited'
    })
    .option('delimiter', {
      type: 'string',
      desc: 'the delimiter for common prefixes (none by default)',
      alias: 'd'
    })
    .option('limit', {
      type: 'number',
      desc: 'the maximum number of values to return',
      alias: 'l'
    })
    .option('page-size', {
      type: 'number',
      desc: 'fetch this many entries per request; max of 1000',
      default: 1000,
      alias: ['page', 'P']
    })
    .option('file', {
      type: 'string',
      desc: 'write out NDJSON file detailing the identified multi-part uploads',
      alias: 'f'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'write details for each multi-part upload to the console',
      alias: 'v'
    })
}

async function handler (args) {
  const {
    bucketOrUri,
    prefix: scanPrefix,
    delimiter,
    limit,
    pageSize,
    file,
    verbose
  } = args

  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)

  let requestCount = 0
  let count = 0
  let exhausted = false
  const notify = throttle({ reportFunc: progressReport })
  const s3 = new aws.S3()
  const watch = durations.stopwatch().start()
  const makeRecords = file || verbose
  const outStream = file ? fs.createWriteStream(file) : undefined

  try {
    console.info(`Listing incomplete multi-part uploads in bucket ${c.blue(bucket)}${prefix ? ` with prefix ${c.yellow(prefix)}` : ''}`)

    await listMore({ bucket, prefix, delimiter, remaining: limit })
    notify({ force: true, halt: true })

    if (exhausted) {
      console.info(`All ${c.orange(count)} entries listed.`)
    } else {
      console.info(`Listed ${c.orange(count)} entries (more available).`)
    }

    if (outStream) {
      outStream.end()
      console.info(`Wrote ${c.orange(count)} records to ${c.blue(file)}`)
    }
  } catch (error) {
    console.error(error)
    console.error(c.red(`Error listing incomplete multi-part uploads. Details above ${c.yellow('^')}`))
    process.exit(1)
  }

  async function listMultipart (options) {
    return new Promise((resolve, reject) => {
      s3.listMultipartUploads(options, (error, data) => {
        error ? reject(error) : resolve(data)
      })
    })
  }

  async function listMore ({ bucket, prefix, delimiter, remaining, keyMarker, idMarker }) {
    const options = {
      Bucket: bucket,
      Prefix: prefix,
      Delimiter: delimiter,
      MaxUploads: remaining ? Math.min(Math.max(0, remaining), pageSize) : undefined,
      KeyMarker: keyMarker,
      UploadIdMarker: idMarker
    }

    const data = await listMultipart(options)

    const {
      Uploads: uploads,
      NextKeyMarker: nextKey,
      NextUploadIdMarker: nextId,
      IsTruncated: truncated
    } = data

    requestCount++
    remaining -= uploads.length
    count += uploads.length

    if (makeRecords) {
      const records = uploads.map(({
        Key: key,
        Initiated: startTime
      }) => {
        const timestamp = moment(startTime).utc()

        return { timestamp, key }
      })

      records.forEach(record => {
        const { timestamp, key } = record

        if (verbose) {
          const age = c.blue(durations.millis(moment().utc().diff(timestamp)))
          const date = c.green(timestamp.format('YYYY-MM-DD'))
          const time = c.yellow(timestamp.format('HH:mm:ss'))
          console.info(`${date} ${time} (${age}) s3://${c.blue(bucket)}/${c.yellow(key)}`)
        }

        if (file) {
          const uri = `s3://${bucket}/${key}`
          const json = JSON.stringify({
            timestamp: timestamp.toISOString(),
            bucket,
            key,
            uri
          })

          outStream.write(`${json}\n`)
        }
      })
    }

    if (remaining != null && remaining < 1) {
      return
    }

    if (!truncated) {
      exhausted = true
      return
    }

    notify()

    if (truncated) {
      await listMore({ bucket, prefix, remaining, keyMarker: nextKey, idMarker: nextId })
    }
  }

  function progressReport () {
    const scanString = `${(requestCount < 2) ? 'in' : 'across'} ${c.orange(requestCount)} scan${(requestCount === 1) ? '' : 's'}`
    console.info(`${c.orange(count)} found ${scanString} (${c.blue(watch)})`)
  }
}
