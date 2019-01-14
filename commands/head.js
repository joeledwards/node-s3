module.exports = {
  command: 'head <bucket> <key>',
  desc: 'fetch metadata for an S3 object',
  builder,
  handler
}

function builder (yargs) {
  yargs
    .positional('bucket', {
      type: 'string',
      desc: 'S3 bucket where the object resides',
      alisa: 'b'
    })
    .positional('key', {
      type: 'string',
      desc: 'S3 key identifying the object whose metadata should be fetched',
      alias: 'k'
    })
}

function handler ({ bucket, key }) {
  const c = require('@buzuli/color')
  const aws = require('aws-sdk')
  const buzJson = require('@buzuli/json')

  const s3 = new aws.S3()

  s3.headObject({
    Bucket: bucket,
    Key: key
  }, (error, data) => {
    if (error) {
      console.error(`Error fetching S3 object : ${error}`)
      process.exit(1)
    } else {
      console.info(`${c.green('s3')}://${c.blue(bucket)}/${c.yellow(key)}`)
      console.info(buzJson(data))
    }
  })
}
