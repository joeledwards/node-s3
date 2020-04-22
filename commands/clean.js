module.exports = {
  command: 'clean <bucket-or-uri> [prefix]',
  desc: 'purge the contents of an S3 bucket or prefix and optional regex key filter',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'the bucket or URI which should be cleaned'
    })
    .positional('prefix', {
      type: 'string',
      desc: 'a prefix to which cleaning should be limited'
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
      desc: 'only remove keys whose names match the regular expression',
      alias: 'k'
    })
    .option('dry-run', {
      type: 'boolean',
      desc: 'do not actually delete anything, just simulate it',
      alias: 'D'
    })
    .option('force', {
      type: 'boolean',
      desc: 'do not prompt before proceeding'
    })
}

async function handler (args) {
  try {
    await clean(args)
  } catch (error) {
    console.error('Fatal:', error)
    process.exit(1)
  }
}

async function clean (args) {
  const c = require('@buzuli/color')
  const aws = require('aws-sdk')
  const inquirer = require('inquirer')
  const promised = require('@buzuli/promised')
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
    verbose,
    dryRun,
    force
  } = args

  const { bucket, key: prefix } = resolveResourceInfo(bucketOrUri, scanPrefix)

  // Output alias with prefix in dry-run mode
  async function log (message) {
    const prefix = dryRun ? c.green('[Dry-Run] ') : ''
    console.info(`${prefix}${message}`)
  }

  const watch = stopwatch().start()
  let lastKey
  let scanned = 0
  let count = 0
  let size = 0

  // Report on progress
  const report = () => {
    const countStr = c.orange(count.toLocaleString())
    const scannedStr = c.orange(scanned.toLocaleString())
    const sizeStr = c.yellow(prettyBytes(size))
    const bytesStr = c.orange(size.toLocaleString())
    const timeStr = c.blue(watch)
    const keyStr = c.yellow(lastKey)
    log(`Deleted ${countStr} of ${scannedStr} keys => ${sizeStr} (${bytesStr} bytes) in ${timeStr} [${keyStr}]`)
  }

  const regex = keyRegex ? new RegExp(keyRegex) : undefined

  const uriStr = `s3://${c.blue(bucket)}/${prefix ? c.yellow(prefix) : ''}`
  const matchStr = regex ? `matching regex /${c.purple(keyRegex)}/ ` : ''

  // Confirm the user wishes to proceed with cleaning
  async function verify () {
    const {
      proceed
    } = await inquirer.prompt([{
      type: 'confirm',
      name: 'proceed',
      default: false,
      message: `Delete all keys at ${uriStr} ${matchStr}?`
    }])

    return proceed
  }

  if (!force) {
    if (!(await verify())) {
      console.info('Understood. Doing nothing.')
      process.exit(0)
    }
  }

  log(`Deleting keys at ${uriStr} ${matchStr} ...`)

  // Configure the reporter
  const noop = () => {}
  const reporter = quiet ? noop : report
  const notify = throttle({
    minDelay: reportFrequency,
    maxDelay: reportFrequency * 2,
    reportFunc: () => reporter()
  })

  const s3 = new aws.S3()

  // Delete a batch of keys
  async function deleteKeys (keys = []) {
    if (keys.length < 1) {
      return []
    }

    if (verbose) {
      log(`Deleting ${c.orange(keys.length)} keys ...`)
    }

    let deletedKeys = keys

    if (!dryRun) {
      const options = {
        Bucket: bucket,
        Delete: {
          Objects: keys.map(key => ({ Key: key })),
          Quiet: false
        }
      }

      // Delete them for real!
      const {
        Deleted: deleted
      } = await promised(h => s3.deleteObjects(options, h))

      deletedKeys = deleted.map(({ Key: key }) => key)
    }

    // Count the number of deletions even for dry-run
    count += keys.length

    return deletedKeys
  }

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

  let keys = []
  for await (const { key, bytes } of scan(bucket, prefix)) {
    const filtered = regex && !key.match(regex)
    scanned++

    if (!filtered) {
      lastKey = key
      size += bytes
      keys.push(key)
    }

    if (verbose) {
      log(`[${filtered ? 'FILTERED' : 'DELETED'}] ${key} ${bytes}`)
    }

    if (keys.length === 1000) {
      await deleteKeys(keys)
      keys = []
    }

    notify()
  }

  // Make sure there are no outstanding keys awaiting deletion
  await deleteKeys(keys)

  notify({ halt: true, reportFunc: () => {} })
  report()
}
