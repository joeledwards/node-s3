module.exports = {
  padString
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
