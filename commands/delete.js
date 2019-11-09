module.exports = {
  command: 'delete <bucket-or-uri> [key]',
  desc: 'delete an s3 resource',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'bucket containing the key or the full uri of the object to delete',
      alisa: 'b'
    })
    .positional('key', {
      type: 'string',
      desc: 'key identifying the object within the bucket',
      alias: 'k'
    })
}

function handler ({
  bucketOrUri,
  key: deleteKey
}) {
  const aws = require('aws-sdk')
  const { resolveResourceInfo } = require('../lib/util')

  const s3 = new aws.S3()

  const { bucket, key } = resolveResourceInfo(bucketOrUri, deleteKey)

  console.info(`Deleting s3://${bucket}/${key} ...`)

  s3.deleteObject({
    Bucket: bucket,
    Key: key
  }, (error, result) => {
    if (error) {
      console.error(`Error deleting S3 object : ${error}`)
    } else {
      console.info('Resource deleted.')
    }
  })
}
