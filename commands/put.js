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
      desc: 'S3 bucket from which to fetch the object'
    })
    .positional('key', {
      type: 'string',
      desc: 'S3 key identifying the object within the bucket'
    })
    .positional('file', {
      type: 'string',
      desc: 'basename of the key by default; use "-" to write to stdout'
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
    .option('part-size', {
      type: 'number',
      desc: 'the maximum size (in MiB) for each part (max buffered in memory per part)',
      default: 20,
      alias: 'p'
    })
    .option('queue-size', {
      type: 'number',
      desc: 'maximum number of outstanding part buffers (buffered sequentially, uploaded in parallel; for fast sources)',
      default: 1,
      alias: 'q'
    })
}

async function handler (args) {
  const r = require('ramda')
  const fs = require('fs')
  const aws = require('aws-sdk')

  const {
    bucket,
    key,
    file,
    stdin,
    header,
    publish,
    partSize,
    queueSize
  } = args

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

  const params = {
    Body: sourceStream,
    Bucket: bucket,
    Key: key
  }

  headers.forEach(([name, value]) => { params[name] = value })

  if (publish) {
    params.ACL = 'public-read'
  }

  const options = {
    partSize: partSize * 1024 * 1024,
    queueSize
  }

  s3.upload(params, options, (error, result) => {
    if (error) {
      console.error(`Error putting S3 object : ${error}`)
      process.exit(1)
    } else {
      const { ETag: etag } = result
      console.info(`S3 put complete [ETag:${etag}].`)
    }
  })
}
