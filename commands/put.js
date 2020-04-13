module.exports = {
  command: 'put <bucket-or-uri> [key]',
  desc: 'write a resource to s3',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'the bucket containing the key or the full uri of the object to write'
    })
    .positional('key', {
      type: 'string',
      desc: 'the key identifying the destination of the object within the bucket'
    })
    .option('file', {
      type: 'string',
      desc: 'input from the named file',
      alias: 'f'
    })
    .option('stdin', {
      type: 'boolean',
      desc: 'stream content from stdin',
      conflicts: ['file'],
      alias: 'S'
    })
    .option('header', {
      type: 'array',
      desc: 'headers to add to the S3 object (e.g. -h "ContentEncoding:gzip")',
      alias: 'h'
    })
    .option('metadata', {
      type: 'array',
      desc: 'metadata to be applied to the object (e.g. -m "git-hash:feedbeef")',
      alias: 'm'
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
    .option('verbose', {
      type: 'boolean',
      desc: '',
      alias: 'v'
    })
}

async function handler (args) {
  const r = require('ramda')
  const fs = require('fs')
  const aws = require('aws-sdk')
  const buzJson = require('@buzuli/json')
  const { resolveResourceInfo } = require('../lib/util')

  const {
    bucketOrUri,
    key: putKey,
    file,
    stdin,
    header,
    metadata,
    publish,
    partSize,
    queueSize,
    verbose
  } = args

  const s3 = new aws.S3()

  const { bucket, key } = resolveResourceInfo(bucketOrUri, putKey)

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

  const metadataRecord = r.compose(
    r.fromPairs,
    r.filter(r.complement(r.isNil)),
    r.map(splitHeader),
    r.filter(r.complement(r.isNil))
  )(metadata || [])

  const headers = r.compose(
    r.filter(r.complement(r.isNil)),
    r.map(splitHeader),
    r.filter(r.complement(r.isNil))
  )(header || [])

  if (verbose) {
    console.info('Headers:')
    console.info(buzJson(headers))
    console.info('Metadata:')
    console.info(buzJson(metadataRecord))
  }

  let sourceStream
  if (stdin) {
    sourceStream = process.stdin
  } else {
    sourceStream = fs.createReadStream(file)
    console.info(`Putting ${file} to s3://${bucket}/${key} ...`)

    if (publish) {
      console.info(`Publicly available at https://${bucket}.s3.amazonaws.com/${key}`)
    }
  }

  const params = {
    Body: sourceStream,
    Bucket: bucket,
    Key: key,
    Metadata: metadataRecord
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
