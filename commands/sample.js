const handler = require('../lib/handler')
const meter = require('@buzuli/meter')

module.exports = {
  command: 'sample <bucket-or-uri> [prefix]',
  desc: 'sample nested keys in JSON records to the requested depth and output regular reports on counts per key',
  builder,
  handler: handler(s3Scan),

  walkRecord
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
    .option('key-filter', {
      type: 'string',
      desc: 'only scan contents of keys whose names match this regular expression',
      alias: 'K'
    })
    .option('path-filter', {
      type: 'string',
      desc: 'filter metrics to paths matching this regular expression',
      alias: 'P'
    })
    .option('leaf-type-filter', {
      type: 'string',
      desc: 'filter to leaf nodes of this type',
      choices: ['ARRAY', 'BOOLEAN', 'NULL', 'NUMBER', 'STRING'],
      alias: 'L'
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
      alias: 'p'
    })
    .option('inspect-arrays', {
      type: 'boolean',
      desc: 'inspect each array item (elements generically represented as $_ARRAY_ITEM_$)',
      alias: 'a'
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
  const buzJson = require('@buzuli/json')
  const throttle = require('@buzuli/throttle')
  const { stopwatch } = require('durations')
  const { resolveResourceInfo } = require('../lib/util')

  const {
    bucketOrUri,
    prefix: scanPrefix,
    keyFilter,
    pathFilter,
    depth,
    parse,
    inspectArrays,
    reportFrequency,
    leafTypeFilter,
    verbose
  } = args

  const parsePaths = parse || []

  const metrics = meter()
  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)
  const keyRegex = keyFilter ? new RegExp(keyFilter) : undefined
  const pathRegex = pathFilter ? new RegExp(pathFilter) : undefined

  console.info({
    bucket,
    prefix,
    depth,
    parsePaths
  })

  const watch = stopwatch()
  let lastKey

  // Report on progress
  const reporter = () => {
    console.info(`${c.blue(watch)} elapsed [last-key => ${lastKey}]`)
    console.info(buzJson(metrics.asObject({ sort: true })))
  }

  if (verbose) {
    const uriStr = `s3://${c.blue(bucket)}/${prefix ? c.yellow(prefix) : ''}`
    const matchStr = keyRegex ? ` matching regex /${c.purple(keyRegex)}/` : ''

    console.info(`Scanning object content at ${uriStr}${matchStr}`)
  }

  // Configure the reporting
  const notify = throttle({
    minDelay: reportFrequency * 1000,
    maxDelay: reportFrequency * 1000,
    reportFunc: () => reporter()
  })

  const keyFilterFunction = key => {
    const filtered = keyRegex && !key.match(keyRegex)
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

  const scanner = ({ key, line }) => {
    metrics.add('scan.lines')
    try {
      const record = JSON.parse(line)
      metrics.add('scan.records.valid')
      walkRecord(record, { metrics, parsePaths, inspectArrays, pathRegex, leafTypeFilter }, { depth })
    } catch (error) {
      console.error(`Error sampling line:\n${line}\n`, error)
      metrics.add('scan.records.invalid')
    }
  }

  const s3 = aws.s3()

  watch.start()
  await s3.scanLogs(bucket, prefix, scanner, { keyFilter: keyFilterFunction })
  watch.stop()

  notify({ halt: true, force: true })
}

function walkRecord (
  record,
  { // options
    pathRegex,
    leafTypeFilter,
    parsePaths = [],
    inspectArrays = false,
    metrics = meter()
  } = {},
  { // context
    depth = 1,
    path = []
  } = {}
) {
  const options = {
    pathRegex,
    leafTypeFilter,
    parsePaths,
    inspectArrays,
    metrics
  }

  let workingRecord = record
  const recordType = typeof record

  const isNull = record == null
  const isBoolean = recordType === 'boolean'
  const isNumber = recordType === 'number'
  const isString = recordType === 'string'
  const isArray = record instanceof Array
  const isObject = !(isNull || isBoolean || isNumber || isString || isArray)

  // The current path segment is not fully defined yet.
  // Extract the parts so we can assemble it later to forward to recursive calls.
  let ancestors = []
  let key

  if (path.length > 0) {
    const [{ key: recordKey }, ...rest] = path
    key = recordKey
    ancestors = rest
  }

  const pathStr = path.map(({ key }) => key).reverse().join('.')
  let display = key
  let leafDisplay

  if (isArray) {
    leafDisplay = `${key}=ARRAY`
  } else if (isObject) {
    leafDisplay = `${key}=OBJECT`
  } else if (isString && parsePaths.includes(pathStr)) {
    try {
      workingRecord = JSON.parse(record)
      if (key != null) {
        display = `${key}=JSON`
        leafDisplay = `${key}=STRING`
      }
    } catch (error) {
      console.warn(`Failed to parse JSON from record @ ${pathStr}:`, record)
      display = `${key}=STRING`
    }
  } else if (isString) {
    display = `${key}=STRING`
  } else if (isNumber) {
    display = `${key}=NUMBER`
  } else if (isBoolean) {
    display = `${key}=BOOLEAN`
  } else if (isNull) {
    display = `${key}=NULL`
  }

  // Fill out this path segment if we are not at the root
  if (key != null) {
    path = [{ key, display, leafDisplay }, ...ancestors]

    let base = path
    let leaf = []

    if (path.length === 1) {
      leaf = path
      base = []
    } else if (path.length > 1) {
      const [l, ...b] = path
      leaf = [l]
      base = b
    }

    const pathDisplayStr = ([
      ...leaf.map(({ display: d, leafDisplay: ld }) => ld || d),
      ...base.map(({ display: d }) => d)
    ]).reverse().join('.')

    if (
      (!pathRegex || pathDisplayStr.match(pathRegex)) &&
      (!leafTypeFilter || pathDisplayStr.endsWith(leafTypeFilter))
    ) {
      metrics.add(`paths:${pathDisplayStr}`)
    }
  }

  if (depth > 0) {
    if (isArray && inspectArrays) {
      workingRecord.forEach((value, idx) => {
        walkRecord(value, options, {
          depth: depth - 1,
          path: [{ key: '$_ARRAY_ITEM_$', display: '$_ARRAY_ITEM_$' }, ...path]
        })
      })
    } else if (isObject) {
      Object.entries(workingRecord).forEach(([key, value]) => {
        walkRecord(value, options, {
          depth: depth - 1,
          path: [{ key, display, leafDisplay }, ...path]
        })
      })
    }
  }

  return metrics
}
