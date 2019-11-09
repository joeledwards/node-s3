module.exports = {
  command: 'get <bucket-or-uri> [key]',
  desc: 'fetch an s3 resource',
  builder,
  handler
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
}

function handler ({
  bucketOrUri,
  key: getKey,
  file,
  stdout
}) {
  const fs = require('fs')
  const aws = require('aws-sdk')
  const path = require('path')
  const { resolveResourceInfo } = require('../lib/util')

  const s3 = new aws.S3()

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
      console.info('S3 fetch complete.')
    }
  })

  s3Stream.pipe(sinkStream)
}
