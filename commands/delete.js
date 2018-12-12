module.exports = {
  command: 'delete <bucket> <key>',
  desc: 'delete an s3 resource',
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
}

function handler ({ bucket, key }) {
  const aws = require('aws-sdk')

  const s3 = new aws.S3()

  console.info(`Deleting s3://${bucket}/${key} ...`)

  s3.deleteObject({
    Bucket: bucket,
    Key: key
  }, (error, result) => {
    if (error) {
      console.error(`Error deleting S3 object : ${error}`)
    } else {
      console.info(`Resource deleted.`)
    }
  })
}
