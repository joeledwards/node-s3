const handler = require('../lib/handler')

module.exports = {
  command: 'sample <bucket-or-uri> [prefix]',
  desc: 'sample nested keys in JSON records to the requested depth and output regular reports on counts per key',
  builder,
  handler: handler(s3Scan)
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
    .option('depth', {
      type: 'number',
      desc: 'sample nested keys in JSON records to the requested depth and output regular reports on counts per key',
      alias: 'd',
      default: 3
    })
    .option('parse', {
      type: 'array',
      desc: 'parse these paths as JSON if encountered as strings',
      alias: 'p',
    })
    .option('report-frequency', {
      type: 'number',
      desc: 'print updates at this frequency (floating point seconds)',
      default: 5,
      alias: 'r'
    })
    .option('verbose', {
      type: 'boolean',
      desc: 'report progress details to stderr',
      alias: 'v'
    })
}

async function s3Scan (options) {
  try {
    await scan(options)
  } catch (error) {
    if (error.code !== 'EPIPE') {
      console.error('Fatal:', error)
    }
    process.exit(1)
  }
}

async function scan ({ aws, options: args }) {
  const c = require('@buzuli/color')
  const meter = require('@buzuli/meter')
  const gunzip = require('gunzip-maybe')
  const buzJson = require('@buzuli/json')
  const promised = require('@buzuli/promised')
  const throttle = require('@buzuli/throttle')
  const prettyBytes = require('pretty-bytes')
  const { stopwatch } = require('durations')
  const { resolveResourceInfo } = require('../lib/util')

  const {
    bucketOrUri,
    prefix: scanPrefix,
    keyRegex,
    depth,
    parse,
    reportFrequency,
    verbose,
  } = args

  const parsePaths = parse || []

  const metrics = meter()
  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)
  const regex = keyRegex ? new RegExp(keyRegex) : undefined

  console.info({
    bucket,
    prefix,
    depth,
    parsePaths
  })

  const watch = stopwatch()
  let lastKey
  let scanCount = 0
  let skipCount = 0
  let bytesRead = 0

  // Report on progress
  const reporter = () => {
    console.info(`${c.blue(watch)} elapsed [last-key => ${lastKey}]`)
    console.info(buzJson(metrics.asObject({ sort: true })))
  }

  if (verbose) {
    const uriStr = `s3://${c.blue(bucket)}/${prefix ? c.yellow(prefix) : ''}`
    const matchStr = regex ? ` matching regex /${c.purple(keyRegex)}/` : ''

    console.info(`Scanning object content at ${uriStr}${matchStr}`)
  }

  // Configure the reporting
  const noop = () => {}
  const notify = throttle({
    minDelay: reportFrequency * 1000,
    maxDelay: reportFrequency * 2 * 1000,
    reportFunc: () => reporter()
  })

  const keyFilter = key => {
    const filtered = regex && !key.match(regex)
    const filterMsg = `${c.red('x=')} ${c.yellow('Filtered')} Key`
    const sampleMsg = `${c.green('<=')} ${c.blue('Sampling')} Key`

    lastKey = key

    if (filtered) {
      metrics.add('s3.keys.filtered')
      if (verbose) {
        console.info(`${filterMsg} : ${key}`)
      }
    } else {
      metrics.add('s3.keys.sampled')
      if (verbose) {
        console.info(`${sampleMsg} : ${key}`)
      }
    }
    return !filtered
  }

  function sample (record, depth = 1, path = []) {
    let shouldRecurse = false
    let workingRecord = record

    // The current path segment is not fully defined yet.
    // Extract the parts so we can assemble it later to forward to recursive calls.
    let ancestors = []
    let key

    if (path.length > 0) {
      const [{ key: recordKey  }, ...rest] = path
      key = recordKey
      ancestors = rest
    }

    const pathStr = path.map(({ key }) => key).reverse().join('.')
    let display = key

    if (depth > 0) {
      let isObject = !(
        (record == null) ||
        (typeof record == 'boolean') ||
        (typeof record == 'number') ||
        (typeof record == 'string') ||
        (record instanceof Array)
      )

      if (isObject) {
        shouldRecurse = true
      } else if (typeof record === 'string' && parsePaths.includes(pathStr)) {
        try {
          workingRecord = JSON.parse(record)
          if (key != null) {
            display = `$${key}`
            shouldRecurse = true
          }
        } catch (error) {
          console.warn(`Failed to parse JSON from record @ ${pathStr}:`, record)
        }
      }

      // Fill out this path segment if we are not at the root
      if (key != null) {
        path = [{ key, display }, ...ancestors]
      }
    }

    if (shouldRecurse) {
      Object.entries(workingRecord).forEach(([key, value]) => {
        sample(value, depth - 1, [{ key }, ...path])
      })
    } else {
      const pathStr = path.map(({ display }) => display).reverse().join('.')
      metrics.add(`paths:${pathStr}`)
    }
  }

  const scanner = ({ key, line }) => {
    metrics.add('scan.lines')
    try {
      const record = JSON.parse(line)
      metrics.add('scan.records.valid')
      sample(record, depth)
    } catch (error) {
      console.error(`Error sampling line:\n${line}\n`, error)
      metrics.add('scan.records.invalid')
    }
  }

  const s3 = aws.s3()

  watch.start()
  await s3.scanLogs(bucket, prefix, scanner, { keyFilter })
  watch.stop()

  notify({ halt: true, force: true })
}
