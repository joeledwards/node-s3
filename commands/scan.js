module.exports = {
  command: 'scan <bucket-or-uri> [prefix]',
  desc: 'scan the content of keys in an S3 bucket, optional prefix and regex key filter',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'the bucket or URI which should be scanned'
    })
    .positional('prefix', {
      type: 'string',
      desc: 'a prefix to which scanning should be limited'
    })
    .option('key-regex', {
      type: 'string',
      desc: 'only scan contents of keys whose names match the regular expression',
      alias: 'k'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'report progress details to stderr',
      alias: 'v'
    })
    .option('report-frequency', {
      type: 'number',
      desc: 'print updates at this frequency (only meaningful when in verbose mode)',
      default: 5000,
      alias: 'r'
    })
}

async function handler (args) {
  try {
    await scan(args)
  } catch (error) {
    if (error.code !== 'EPIPE') {
      console.error('Fatal:', error)
    }
    process.exit(1)
  }
}

async function scan (args) {
  const c = require('@buzuli/color')
  const aws = require('aws-sdk')
  const gunzip = require('gunzip-maybe')
  const promised = require('@buzuli/promised')
  const throttle = require('@buzuli/throttle')
  const prettyBytes = require('pretty-bytes')
  const { stopwatch } = require('durations')
  const { resolveResourceInfo } = require('../lib/util')

  const {
    bucketOrUri,
    prefix: scanPrefix,
    keyRegex,
    verbose,
    reportFrequency
  } = args

  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)
  const regex = keyRegex ? new RegExp(keyRegex) : undefined

  const watch = stopwatch()
  let lastKey
  let scanCount = 0
  let skipCount = 0
  let bytesRead = 0

  // Report on progress
  const report = () => {
    const totalCount = scanCount + skipCount
    const totalStr = c.orange(totalCount.toLocaleString())
    const scanStr = c.orange(scanCount.toLocaleString())
    const sizeStr = c.yellow(prettyBytes(bytesRead))
    const timeStr = c.blue(watch)
    const keyStr = c.yellow(lastKey)
    console.error(`Scanned ${scanStr} of ${totalStr} keys => ${sizeStr} in ${timeStr} [${keyStr}]`)
  }

  if (verbose) {
    const uriStr = `s3://${c.blue(bucket)}/${prefix ? c.yellow(prefix) : ''}`
    const matchStr = regex ? ` matching regex /${c.purple(keyRegex)}/` : ''

    console.error(`Scanning object content at ${uriStr}${matchStr}`)
  }

  // Configure the reporting
  const noop = () => {}
  const reporter = verbose ? report : noop
  const notify = throttle({
    minDelay: reportFrequency,
    maxDelay: reportFrequency * 2,
    reportFunc: () => reporter()
  })

  const s3 = new aws.S3()

  // Scan keys from the prefix on S3
  async function * scan (bucket, prefix) {
    let token
    let hasMore = true

    while (hasMore) {
      const options = {
        Bucket: bucket
      }

      if (prefix) {
        options.Prefix = prefix
      }

      if (token) {
        options.ContinuationToken = token
      }

      const result = await promised(h => s3.listObjectsV2(options, h))

      const {
        Contents: keys,
        IsTruncated: isTruncated,
        NextContinuationToken: nextToken
      } = result

      for (const { Key: key, Size: bytes } of keys) {
        yield { key, bytes }
      }

      hasMore = isTruncated
      token = nextToken
    }
  }

  async function scanKey (bucket, key) {
    return new Promise((resolve, reject) => {
      try {
        const outStream = process.stdout

        if (verbose) {
          console.error(`Scanning Key : ${key}`)
        }

        const objStream = s3.getObject({
          Bucket: bucket,
          Key: key
        }).createReadStream()

        const gunzipStream = gunzip()

        const errorHandler = error => reject(error)

        objStream.once('error', errorHandler)
        outStream.once('error', errorHandler)
        gunzipStream.once('error', errorHandler)

        const completed = () => {
          objStream.removeAllListeners()
          gunzipStream.removeAllListeners()
          outStream.removeListener('error', errorHandler)
          resolve()
        }

        // This is the stream we forward
        const inStream = objStream.pipe(gunzipStream)

        const handleDrain = () => inStream.resume()

        const forwardData = data => {
          bytesRead += data.length
          return outStream.write(data)
        }

        const handleData = data => {
          if (!forwardData(data)) {
            // The write method buffered our data, but
            // signalled for us to wait for a drain event
            inStream.pause()
            outStream.once('drain', handleDrain)
          }
        }

        inStream.on('data', handleData)

        inStream.once('end', () => {
          inStream.removeListener('data', handleData)
          completed()
        })
      } catch (error) {
        reject(error)
      }
    })
  }

  watch.start()
  for await (const { key } of scan(bucket, prefix)) {
    const filtered = regex && !key.match(regex)

    if (!filtered) {
      scanCount++
      lastKey = key
      await scanKey(bucket, key)
    } else {
      skipCount++
    }

    notify()
  }

  notify({ halt: true, force: true })
}
