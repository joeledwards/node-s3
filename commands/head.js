const handler = require('../lib/handler')

module.exports = {
  command: 'head <bucket-or-uri> [key]',
  desc: 'fetch metadata for an S3 object',
  builder,
  handler: handler(head)
}

function builder (yargs) {
  yargs
    .positional('bucket-or-uri', {
      type: 'string',
      desc: 'the bucket containing the key or the full uri of the object to inspect',
      alisa: 'b'
    })
    .positional('key', {
      type: 'string',
      desc: 'the key identifying the object whose metadata should be fetched',
      alias: 'k'
    })
    .option('acl', {
      type: 'boolean',
      desc: 'also fetch the ACL for this object',
      alias: 'a'
    })
}

function head ({
  aws,
  options: {
    bucketOrUri,
    key: headKey,
    acl
  }
}) {
  const c = require('@buzuli/color')
  const buzJson = require('@buzuli/json')
  const { resolveResourceInfo } = require('../lib/util')

  const s3 = aws.s3().sdk

  const { bucket, key } = resolveResourceInfo(bucketOrUri, headKey)

  s3.headObject({
    Bucket: bucket,
    Key: key
  }, (error, data) => {
    if (error) {
      console.error(`Error fetching S3 object : ${error}`)
      process.exit(1)
    } else {
      console.info(`${c.green('s3')}://${c.blue(bucket)}/${c.yellow(key)}`)
      try {
        data.LastModified = data.LastModified.toISOString()
      } catch (ignore) {
      }
      console.info(buzJson(data))

      if (acl) {
        s3.getObjectAcl({
          Bucket: bucket,
          Key: key
        }, (error, data) => {
          if (error) {
            console.error(`Error fetching object ACL: ${error}`)
            process.exit(1)
          } else {
            console.info('Object ACL:')
            console.info(buzJson(data))
          }
        })
      }
    }
  })
}
