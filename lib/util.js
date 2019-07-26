module.exports = {
  formatUri,
  padString,
  parseUri,
  trim,
  trimLeft,
  trimRight
}

const url = require('url')

function formatUri (bucket, key) {
  const b = trim('/')(bucket)
  const k = trimRight('/', { keep: 1 })(trimLeft('/')(key))

  return `s3://${b}/${k}`
}

function padString (count, text, left = true, char = ' ') {
  const deficit = count - text.length
  const padding = (deficit > 0) ? char.repeat(deficit) : ''

  return deficit < 1
    ? text
    : left
      ? padding + text
      : text + padding
}

function parseUri (uri) {
  const {
    protocol,
    host,
    path
  } = url.parse(uri)

  let bucket
  let key

  if (protocol === 's3:') {
    if (host) {
      bucket = host

      if (path && path !== '/') {
        key = trimLeft('/')(path)
      }
    } else {
      bucket = trimLeft('/')(path)
    }
  } else {
    bucket = trimLeft('/')(uri)
  }

  if (!key) {
    const pivot = bucket.indexOf('/')

    if (pivot > 0) {
      key = trimLeft('/')(bucket.slice(pivot))
      bucket = bucket.slice(0, pivot)
    }
  }

  key = key ? trimRight('/', { keep: 1 })(key) : undefined
  bucket = trim('/')(bucket)

  const result = {}

  if (bucket) {
    result.bucket = bucket
  }

  if (key) {
    result.key = key
  }

  return result
}

function trimRight (delimeter = ' ', options = {}) {
  return trim(delimeter, { ...options, left: false, right: true })
}

function trimLeft (delimeter = ' ', options = {}) {
  return trim(delimeter, { ...options, left: true, right: false })
}

function trim (delimeter = ' ', {
  left = true,
  right = true,
  keep = 0
} = {}) {
  return text => {
    text = text || ''
    let firstIndex = 0
    let lastIndex = text.length
    const keepLeft = keep
    const keepRight = keep

    // Calculate left trim index
    if (left) {
      while (text[firstIndex] === delimeter) {
        firstIndex++
      }

      firstIndex = Math.max(firstIndex - keepLeft, 0)
    }

    // Calculate right trim index
    if (right) {
      while (text[lastIndex - 1] === delimeter) {
        lastIndex--
      }

      lastIndex = Math.min(lastIndex + keepRight, text.length)
    }

    // If there is no trimming to apply, return the original string
    if ((lastIndex - firstIndex) === text.length) {
      return text
    }

    // If our trimming overlaps, return the empty string
    if (firstIndex >= lastIndex) {
      return ''
    }

    // Otherwise slice to the identified trim indices
    return text.slice(firstIndex, lastIndex)
  }
}
