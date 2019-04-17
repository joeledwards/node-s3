module.exports = {
  command: 'parse-uri <uri>',
  desc: 'translate an S3 URI to "<bucket> <key>" format',
  handler
}

function handler ({ uri }) {
  function bail () {
    console.error('Invalid S3 URI format')
    process.exit(1)
  }

  if (uri.indexOf('s3://') !== 0) {
    bail()
  }

  const clipped = uri.slice(5)
  const endBucket = clipped.indexOf('/')

  if (endBucket < 1) {
    bail()
  }

  const bucket = clipped.slice(0, endBucket)
  const key = clipped.slice(endBucket + 1)

  if (key === '') {
    bail()
  }

  console.info(`${bucket} ${key}`)
}
