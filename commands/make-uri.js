module.exports = {
  command: 'make-uri <bucket> <key>',
  desc: 'translate an S3 URI to "<bucket> <key>" format',
  handler
}

function handler ({ bucket, key }) {
  if (bucket === '') {
    console.error('Invalid bucket')
    process.exit(1)
  }

  if (key === '') {
    console.error('Invalid key')
    process.exit(1)
  }

  console.info(`s3://${bucket}/${key}`)
}
