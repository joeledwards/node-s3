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
}

function handler ({ bucket, key, file }) {
  const fs = require('fs')
  const aws = require('aws-sdk')
  const path = require('path')

  const s3 = new aws.S3()
  if (!file) {
    file = path.basename(key)
  }

  console.info(`Fetching s3://${bucket}/${key} to ${file} ...`)

  const stdout = (file === '-')
  const sinkStream = stdout ? process.stdout : fs.createWriteStream(file)
  const s3Stream =  s3.getObject({
    Bucket: bucket,
    Key: key
  }).createReadStream()

  if (stdout) {
    sinkStream.on('error', error => {
      console.error(`Error writing file : ${error}`)
      process.exit(1)
    })
  }

  s3Stream.on('error', error => {
    console.error(`Error fetching S3 object : ${error}`)
    process.exit(1)
  })

  s3Stream.on('end', () => {
    console.info(`S3 fetch complete.`)
  })

  s3Stream.pipe(sinkStream)
}
