module.exports = {
  command: 'get <bucket> <key> [file]',
  desc: 'fetch an s3 resource',
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
    .option('stdout', {
      type: 'boolean',
      desc: 'write to stdout instead of a file (will ignore file name)',
      default: false,
      alias: 'S'
    })
}

function handler ({ bucket, key, file, stdout }) {
  const fs = require('fs')
  const aws = require('aws-sdk')
  const path = require('path')

  const s3 = new aws.S3()

  let sinkStream
  if (stdout) {
    sinkStream = process.stdout
  } else {
    if (!file) {
      file = path.basename(key)
    }
    sinkStream = fs.createWriteStream(file)
    sinkStream.on('error', error => {
      console.error(`Error writing file : ${error}`)
      process.exit(1)
    })
    console.info(`Fetching s3://${bucket}/${key} to ${file} ...`)
  }

  const s3Stream = s3.getObject({
    Bucket: bucket,
    Key: key
  }).createReadStream()

  s3Stream.on('error', error => {
    console.error(`Error fetching S3 object : ${error}`)
    process.exit(1)
  })

  s3Stream.on('end', () => {
    if (!stdout) {
      console.info(`S3 fetch complete.`)
    }
  })

  s3Stream.pipe(sinkStream)
}
