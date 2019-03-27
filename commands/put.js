module.exports = {
  command: 'put <bucket> <key> [file]',
  desc: 'write a resource to s3',
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
    .positional('key', {
      type: 'string',
      desc: 'S3 key identifying the object within the bucket',
      alias: 'k'
    })
    .positional('file', {
      type: 'string',
      desc: 'basename of the key by default; use "-" to write to stdout',
      alias: 'f'
    })
    .option('stdin', {
      type: 'boolean',
      desc: 'stream content to stdin',
      default: false,
      alias: 'S'
    })
    .option('header', {
      type: 'array',
      desc: 'headers to add to the S3 object (e.g. -h "ContentEncoding:gzip")',
      alias: 'h'
    })
    .option('publish', {
      type: 'boolean',
      desc: 'make the resource public (read-only)',
      default: false,
      alias: 'P'
    })
}

async function collectStream (stream) {
  return new Promise((resolve, reject) => {
    const buffer = []
    stream.on('data', data => buffer.push(data))
    stream.once('end', () => resolve(Buffer.concat(buffer)))
    stream.once('error', reject)
  })
}

async function handler ({ bucket, key, file, stdin, header, publish }) {
  const r = require('ramda')
  const fs = require('fs')
  const aws = require('aws-sdk')

  const s3 = new aws.S3()

  const splitHeader = header => {
    if (!header) return
    if (typeof header !== 'string') return

    const pivot = header.indexOf(':')

    if (pivot < 1) return

    const name = r.trim(header.substr(0, pivot))

    if (name.length < 1) return
    if (pivot + 1 > header.length) return

    const value = r.trim(header.substr(pivot + 1))

    if (value.length < 1) return

    return [name, value]
  }

  if (!stdin && !file) {
    console.error('Must either specify a file or stream to stdin.')
    process.exit(1)
  }

  const headers = r.compose(
    r.filter(r.complement(r.isNil)),
    r.map(splitHeader),
    r.filter(r.complement(r.isNil))
  )(header || [])

  let sourceStream
  if (stdin) {
    sourceStream = process.stdin
  } else {
    sourceStream = fs.createReadStream(file)
    console.info(`Putting ${file} to s3://${bucket}/${key} ...`)
  }

  let body
  try {
    // TODO: if file, determine size first
    // TODO: if stdin, send in 100MB chunks
    body = await collectStream(sourceStream)
  } catch (error) {
    console.error('Error collecting data:', error)
  }

  const options = {
    Body: body,
    Bucket: bucket,
    Key: key
  }

  headers.forEach(([name, value]) => { options[name] = value })

  if (publish) {
    options.ACL = 'public-read'
  }

  s3.putObject(options, (error, result) => {
    if (error) {
      console.error(`Error putting S3 object : ${error}`)
      process.exit(1)
    } else {
      const { ETag: etag } = result
      console.info(`S3 put complete [ETag:${etag}].`)
    }
  })
}
