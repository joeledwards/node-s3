const tap = require('tap')

const {
  formatUri,
  padString,
  parseUri,
  resolveResourceInfo,
  trim,
  trimLeft,
  trimRight
} = require('../lib/util')

tap.test('util.formatUri()', async assert => {
  assert.equal(formatUri('bar'), 's3://bar/')
  assert.equal(formatUri('bar', 'foo'), 's3://bar/foo')
  assert.equal(formatUri('bar', '/foo'), 's3://bar/foo')
  assert.equal(formatUri('bar', 'foo/'), 's3://bar/foo/')
  assert.equal(formatUri('bar', '/foo/'), 's3://bar/foo/')
  assert.equal(formatUri('/bar', '/foo/'), 's3://bar/foo/')
  assert.equal(formatUri('//bar', '/foo/'), 's3://bar/foo/')
  assert.equal(formatUri('bar/', '/foo/'), 's3://bar/foo/')
  assert.equal(formatUri('bar//', '/foo/'), 's3://bar/foo/')
  assert.equal(formatUri('/bar/', '/foo/'), 's3://bar/foo/')
  assert.equal(formatUri('//bar//', '/foo/'), 's3://bar/foo/')
})

tap.test('util.padString()', async assert => {
  assert.equal(padString(5, 'foo'), '  foo')
  assert.equal(padString(4, 'foo'), ' foo')
  assert.equal(padString(3, 'foo'), 'foo')
  assert.equal(padString(2, 'foo'), 'foo')

  assert.equal(padString(5, 'foo', false), 'foo  ')
  assert.equal(padString(4, 'foo', false), 'foo ')
  assert.equal(padString(3, 'foo', false), 'foo')
  assert.equal(padString(2, 'foo', false), 'foo')
})

tap.test('util.parseUri()', async assert => {
  assert.same(parseUri('s3://'), {})

  assert.same(parseUri('s3://bkt/key'), { bucket: 'bkt', key: 'key' })
  assert.same(parseUri('s3://bkt/k/p'), { bucket: 'bkt', key: 'k/p' })
  assert.same(parseUri('s3://bkt/k/p/'), { bucket: 'bkt', key: 'k/p/' })
  assert.same(parseUri('s3://bkt/'), { bucket: 'bkt' })
  assert.same(parseUri('s3://bkt'), { bucket: 'bkt' })

  assert.same(parseUri('/bkt'), { bucket: 'bkt' })
  assert.same(parseUri('/bkt/'), { bucket: 'bkt' })
  assert.same(parseUri('/bkt/k/p'), { bucket: 'bkt', key: 'k/p' })
  assert.same(parseUri('/bkt/k/p/'), { bucket: 'bkt', key: 'k/p/' })
  assert.same(parseUri('bkt/'), { bucket: 'bkt' })
  assert.same(parseUri('bkt/key'), { bucket: 'bkt', key: 'key' })
  assert.same(parseUri('bkt/key/'), { bucket: 'bkt', key: 'key/' })
  assert.same(parseUri('bkt/k/p'), { bucket: 'bkt', key: 'k/p' })
  assert.same(parseUri('bkt/k/p/'), { bucket: 'bkt', key: 'k/p/' })
  assert.same(parseUri('bkt/k/p//'), { bucket: 'bkt', key: 'k/p/' })
})

tap.test('util.resolveResourceInfo()', async assert => {
  assert.same(resolveResourceInfo('bkt', 'key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('bkt', 'key/'), { bucket: 'bkt', key: 'key/' })
  assert.same(resolveResourceInfo('bkt', 'key/stuff'), { bucket: 'bkt', key: 'key/stuff' })

  assert.same(resolveResourceInfo('bkt/ignored', 'key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('/bkt/ignored', 'key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('//bkt/ignored', 'key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('s3://bkt/ignored', 'key'), { bucket: 'bkt', key: 'key' })

  assert.same(resolveResourceInfo('/bkt'), { bucket: 'bkt' })
  assert.same(resolveResourceInfo('bkt/key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('/bkt/key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('//bkt/key'), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('s3://bkt/key'), { bucket: 'bkt', key: 'key' })

  assert.same(resolveResourceInfo('s3://bkt/key', undefined), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('s3://bkt/key', null), { bucket: 'bkt', key: 'key' })
  assert.same(resolveResourceInfo('s3://bkt/key', ''), { bucket: 'bkt', key: 'key' })
})

tap.test('util.trim()', async assert => {
  assert.equal(trim()('  foo'), 'foo')
  assert.equal(trim()(' foo'), 'foo')
  assert.equal(trim()('foo'), 'foo')
  assert.equal(trim()('foo '), 'foo')
  assert.equal(trim()('foo  '), 'foo')
  assert.equal(trim()(' foo '), 'foo')
  assert.equal(trim()('  foo  '), 'foo')

  assert.equal(trim('/')('/path'), 'path')
  assert.equal(trim('/')('path/'), 'path')
  assert.equal(trim('/')(' /path'), ' /path')
  assert.equal(trim('/')('//path'), 'path')
  assert.equal(trim('/')('/ path'), ' path')
  assert.equal(trim('/')('path /'), 'path ')
  assert.equal(trim('/')('path//'), 'path')
  assert.equal(trim('/')('path/ '), 'path/ ')

  assert.equal(trim(' ')('   '), '')
  assert.equal(trim('/')('///'), '')

  assert.equal(trim(':', { keep: 1 })('foo'), 'foo')
  assert.equal(trim(':', { keep: 1 })(':foo'), ':foo')
  assert.equal(trim(':', { keep: 1 })('foo:'), 'foo:')
  assert.equal(trim(':', { keep: 1 })(':foo:'), ':foo:')
  assert.equal(trim(':', { keep: 1 })('::foo:'), ':foo:')
  assert.equal(trim(':', { keep: 1 })(':foo::'), ':foo:')
  assert.equal(trim(':', { keep: 1 })('::foo::'), ':foo:')
})

tap.test('util.trimLeft()', async assert => {
  assert.equal(trimLeft()('  foo'), 'foo')
  assert.equal(trimLeft()('foo  '), 'foo  ')
  assert.equal(trimLeft()('  foo  '), 'foo  ')

  assert.equal(trimLeft()('   '), '')

  assert.equal(trimLeft('/', { keep: 2 })('//foo'), '//foo')
})

tap.test('util.trimRight()', async assert => {
  assert.equal(trimRight()('  foo'), '  foo')
  assert.equal(trimRight()('foo  '), 'foo')
  assert.equal(trimRight()('  foo  '), '  foo')

  assert.equal(trimRight()('   '), '')

  assert.equal(trimRight('/', { keep: 2 })('foo///'), 'foo//')
})
