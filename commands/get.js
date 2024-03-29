const handler = require('../lib/handler')

module.exports = {
  command: 'get <bucket-or-uri> [key]',
  desc: 'fetch an s3 resource',
  builder,
  handler: handler(get)
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'bucket containing the key or the full uri of the object to fetch',
      alisa: 'b'
    })
    .positional('key', {
      type: 'string',
      desc: 'key identifying the object to fetch',
      alias: 'k'
    })
    .option('file', {
      type: 'string',
      desc: 'path of the file to which output should be written (basename of the key is used as the filename by default)',
      alias: 'f'
    })
    .option('stdout', {
      type: 'boolean',
      desc: 'write to stdout instead of a file (will ignore file name)',
      default: false,
      alias: 'S'
    })
    .option('range', {
      type: 'string',
      desc: 'the inclusive byte range to download (e.g., "0-499"); remember to quote the range',
      coerce: validateRange,
      alias: 'R'
    })
}

function validateRange (range) {
  const invalid = () => {
    throw new Error(`Invalid range: "${range}"`)
  }

  if (range == null) {
    return undefined
  }

  const parts = range.match(/^(\d+)(?:-(\d+))?$/)

  if (!parts) {
    invalid()
  }

  const [, start, end] = parts.map(n => Number.parseInt(n))

  if (Number.isNaN(start)) {
    invalid()
  }

  if (Number.isNaN(end)) {
    return `${start}-${start}`
  }

  if (start > end) {
    invalid()
  }

  return range
}

function get ({
  aws,
  options: {
    bucketOrUri,
    key: getKey,
    file,
    stdout,
    range
  }
}) {
  const fs = require('fs')
  const path = require('path')
  const { resolveResourceInfo } = require('../lib/util')

  const s3 = aws.s3().sdk

  const { bucket, key } = resolveResourceInfo(bucketOrUri, getKey)

  let sinkStream
  if (stdout) {
    sinkStream = process.stdout
    sinkStream.on('error', error => {
      if (error.code !== 'EPIPE') {
        console.error(`Error writing to stdout : ${error}`)
        process.exit(1)
      }
    })
  } else {
    if (!file) {
      file = path.basename(key)
    }
    sinkStream = fs.createWriteStream(file)
    sinkStream.on('error', error => {
      if (error.code !== 'EPIPE') {
        console.error(`Error writing file : ${error}`)
        process.exit(1)
      }
    })
    console.info(`Fetching s3://${bucket}/${key} to ${file} ...`)
  }

  const params = {
    Bucket: bucket,
    Key: key
  }

  if (range) {
    params.Range = `bytes=${range}`
  }

  const s3Stream = s3.getObject(params).createReadStream()

  s3Stream.on('error', error => {
    console.error(`Error fetching S3 object : ${error}`)
    process.exit(1)
  })

  s3Stream.on('end', () => {
    if (!stdout) {
      console.info('S3 fetch complete.')
    }
  })

  s3Stream.pipe(sinkStream)
}
