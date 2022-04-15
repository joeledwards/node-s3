const handler = require('../lib/handler')

const c = require('@buzuli/color')
const fs = require('fs')
const moment = require('moment')
const throttle = require('@buzuli/throttle')
const durations = require('durations')
const { resolveResourceInfo } = require('../lib/util')

module.exports = {
  command: 'list-multipart <bucket-or-uri> [prefix]',
  desc: 'list incomplete multi-part uploads',
  builder,
  handler: handler(listMultipart)
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
    .option('size', {
      type: 'boolean',
      desc: 'show size of each key',
      alias: 's'
    })
    .option('show-ids', {
      type: 'boolean',
      desc: 'show multipart upload IDs',
      alias: ['ids', 'i']
    })
    .option('extended', {
      type: 'boolean',
      desc: 'fetch extended MPU info (results in an additional request per MPU to fetch part info)',
      alias: 'x'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'write details for each multi-part upload to the console',
      alias: 'v'
    })
}

async function listMultipart ({ aws, options: args }) {
  const {
    bucketOrUri,
    prefix: scanPrefix,
    delimiter,
    limit,
    pageSize,
    file,
    size,
    showIds,
    extended,
    verbose
  } = args

  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)

  const prettyBytes = require('pretty-bytes')

  let requestCount = 0
  let count = 0
  let totalSize = 0
  let exhausted = false
  const notify = throttle({ reportFunc: progressReport })
  const s3 = aws.s3().sdk
  const watch = durations.stopwatch().start()
  const makeRecords = file || verbose || extended || size
  const outStream = file ? fs.createWriteStream(file) : undefined

  try {
    console.info(`Listing incomplete multi-part uploads in bucket ${c.blue(bucket)}${prefix ? ` with prefix ${c.yellow(prefix)}` : ''}`)

    await listMore({ bucket, prefix, delimiter, remaining: limit })
    notify({ force: true, halt: true })

    const sizeInfo = (extended || size) ? ` => ${c.yellow(prettyBytes(totalSize))} (${c.orange(totalSize.toLocaleString())} bytes)` : ''

    if (exhausted) {
      console.info(`All ${c.orange(count)} entries listed${sizeInfo}`)
    } else {
      console.info(`Listed ${c.orange(count)} entries (more available)${sizeInfo}`)
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

  async function listParts (options) {
    return new Promise((resolve, reject) => {
      s3.listParts(options, (error, data) => {
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
        Initiated: startTime,
        UploadId: uploadId
      }) => {
        const timestamp = moment(startTime).utc()

        return { timestamp, key, uploadId }
      })

      for (const record of records) {
        const { timestamp, key, uploadId } = record

        let partList
        let bytes

        if (extended || size) {
          bytes = 0

          const listingOptions = {
            Bucket: bucket,
            Key: key,
            UploadId: uploadId
          }

          const partsData = await listParts(listingOptions)

          const parts = partsData.Parts.map(({
            ETag: etag,
            LastModified: lastModified,
            PartNumber: number,
            Size: size
          }) => {
            bytes = (bytes || 0) + size
            totalSize += size
            const modified = moment.utc(lastModified)
            return { etag, modified, number, size }
          })

          if (extended) {
            parts.forEach(({ etag, modified, number, size }) => {
              const age = c.blue(durations.millis(moment().utc().diff(modified)))
              const date = c.green(modified.format('YYYY-MM-DD'))
              const time = c.yellow(modified.format('HH:mm:ss'))
              const sizeStr = c.yellow(prettyBytes(size))
              const bytesStr = c.orange(size.toLocaleString())
              const sizeInfo = ` => ${sizeStr} (${bytesStr} bytes)`
              console.info(`${date} ${time} (${age}) ${c.white(number)} [${c.purple(etag)}]${sizeInfo}`)
            })
          }

          partList = parts.map(({ etag, modified, number, size }) => {
            return { etag, modified: modified.toISOString(), number, size }
          })
        }

        if (verbose || extended || showIds) {
          const age = c.blue(durations.millis(moment().utc().diff(timestamp)))
          const date = c.green(timestamp.format('YYYY-MM-DD'))
          const time = c.yellow(timestamp.format('HH:mm:ss'))
          let sizeInfo = ''
          if (bytes != null) {
            const sizeStr = c.yellow(prettyBytes(bytes))
            const bytesStr = c.orange(bytes.toLocaleString())
            sizeInfo = ` => ${sizeStr} (${bytesStr} bytes)`
          }
          let idInfo = ''
          if (showIds) {
            idInfo = `\n    UploadId : ${c.yellow(uploadId)}`
          }
          console.info(`${date} ${time} (${age}) s3://${c.blue(bucket)}/${c.yellow(key)}${sizeInfo}${idInfo}`)
        }

        if (file) {
          const uri = `s3://${bucket}/${key}`
          const json = JSON.stringify({
            timestamp: timestamp.toISOString(),
            bucket,
            key,
            uri,
            parts: partList
          })

          outStream.write(`${json}\n`)
        }
      }
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
