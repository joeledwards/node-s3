const handler = require('../lib/handler')

module.exports = {
  command: 'size <bucket-or-uri> [prefix]',
  desc: 'count the bytes and objects at an S3 location',
  builder,
  handler: handler(s3Size)
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
    .option('quiet', {
      type: 'boolean',
      desc: 'only print output on completion or error',
      default: false,
      alias: 'q'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'list every key (for debugging)',
      default: false,
      alias: 'v'
    })
    .option('report-frequency', {
      type: 'number',
      desc: 'print updates at this frequency',
      default: 5000,
      alias: 'r'
    })
    .option('key-regex', {
      type: 'string',
      desc: 'only count keys whose names match the regular expression',
      alias: 'k'
    })
}

async function s3Size ({ aws, options: args }) {
  const c = require('@buzuli/color')
  const throttle = require('@buzuli/throttle')
  const prettyBytes = require('pretty-bytes')
  const { stopwatch } = require('durations')
  const { resolveResourceInfo } = require('../lib/util')

  const {
    bucketOrUri,
    prefix: scanPrefix,
    keyRegex,
    quiet,
    reportFrequency,
    verbose
  } = args

  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)

  let lastKey
  let scanned = 0
  let count = 0
  let size = 0

  const report = () => {
    const countStr = c.orange(count.toLocaleString())
    const scannedStr = c.orange(scanned.toLocaleString())
    const sizeStr = c.yellow(prettyBytes(size))
    const bytesStr = c.orange(size.toLocaleString())
    const timeStr = c.blue(watch)
    const keyStr = c.yellow(lastKey)
    console.info(`${countStr} of ${scannedStr} keys => ${sizeStr} (${bytesStr} bytes) in ${timeStr} [${keyStr}]`)
  }
  const reporter = () => throttle({
    minDelay: reportFrequency,
    maxDelay: reportFrequency * 2,
    reportFunc: () => report()
  })

  const regex = keyRegex ? new RegExp(keyRegex) : undefined
  const watch = stopwatch().start()
  const noop = () => {}
  const notify = quiet ? noop : reporter()

  const scanner = scan({ aws, bucket, prefix })

  scanner.on('error', error => {
    console.error('Error counting data volume:', error)
    process.exit(1)
  })

  scanner.on('end', () => {
    notify({ halt: true, reportFunc: () => {} })
    report()
  })

  scanner.on('key', objectMetadata => {
    const { key, bytes } = objectMetadata
    scanned++
    const filtered = regex && !key.match(regex)

    if (!filtered) {
      lastKey = key
      count++
      size += bytes
    }

    if (verbose) {
      console.info(`[${filtered ? 'FILTERED' : 'COUNTED'}] ${key} ${bytes}`)
    }

    notify()
  })
}

function scan ({ aws, bucket, prefix }) {
  const EventEmitter = require('events')

  const s3 = aws.s3().sdk
  const events = new EventEmitter()

  const listMore = token => {
    const options = {
      Bucket: bucket
    }

    if (prefix) {
      options.Prefix = prefix
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
          NextContinuationToken: token
        } = result

        keys.forEach(({
          Key: key,
          Size: bytes
        }) => {
          events.emit('key', { key, bytes })
        })

        if (isTruncated) {
          listMore(token)
        } else {
          events.emit('end')
        }
      }
    })
  }

  listMore()

  return events
}
