const tap = require('tap')

const { walkRecord } = require('../commands/sample')

tap.test('sample.walkRecord() empty object', async assert => {
  assert.equal(walkRecord({}, {}, { depth: 1 }).isMeter, true)
  assert.same(walkRecord({}, {}, { depth: 1 }).asObject(), {})
})

tap.test('sample.walkRecord() type handling', async assert => {
  assert.same(walkRecord({ a: null }, {}, { depth: 2 }).asObject(), { 'paths:a=NULL': 1 })
  assert.same(walkRecord({ a: true }, {}, { depth: 2 }).asObject(), { 'paths:a=BOOLEAN': 1 })
  assert.same(walkRecord({ a: false }, {}, { depth: 2 }).asObject(), { 'paths:a=BOOLEAN': 1 })
  assert.same(walkRecord({ a: '' }, {}, { depth: 2 }).asObject(), { 'paths:a=STRING': 1 })
  assert.same(walkRecord({ a: 'meh' }, {}, { depth: 2 }).asObject(), { 'paths:a=STRING': 1 })
  assert.same(walkRecord({ a: [] }, {}, { depth: 2 }).asObject(), { 'paths:a=ARRAY': 1 })
  assert.same(walkRecord({ a: ['meh '] }, {}, { depth: 2 }).asObject(), { 'paths:a=ARRAY': 1 })
  assert.same(walkRecord({ a: { } }, {}, { depth: 2 }).asObject(), { 'paths:a=OBJECT': 1 })
})

tap.test('sample.walkRecord() path handling', async assert => {
  // NOTE: arrayInspection is not enabled

  assert.same(
    walkRecord({ a: { b: { c: 'v' } } }, {}, { depth: 1 }).asObject(),
    { 'paths:a=OBJECT': 1 }
  )

  assert.same(
    walkRecord({ a: { b: { c: 'v' } } }, {}, { depth: 2 }).asObject(),
    { 'paths:a=OBJECT': 1, 'paths:a.b=OBJECT': 1 }
  )

  assert.same(
    walkRecord({ a: { b: { c: 'v' } } }, {}, { depth: 3 }).asObject(),
    { 'paths:a=OBJECT': 1, 'paths:a.b=OBJECT': 1, 'paths:a.b.c=STRING': 1 }
  )

  assert.same(
    walkRecord({ a: { b: ['v'] } }, {}, { depth: 2 }).asObject(),
    { 'paths:a=OBJECT': 1, 'paths:a.b=ARRAY': 1 }
  )

  assert.same(
    walkRecord({ a: { b: ['v'] } }, {}, { depth: 3 }).asObject(),
    { 'paths:a=OBJECT': 1, 'paths:a.b=ARRAY': 1 }
  )
})

tap.test('sample.walkRecord() array inspection', async assert => {
  // Doubly nested arrays
  assert.same(
    walkRecord({ a: [[{ b: 'v' }]] }, { inspectArrays: true }, { depth: 5 }).asObject(),
    {
      'paths:a=ARRAY': 1,
      'paths:a.$_ARRAY_ITEM_$=ARRAY': 1,
      'paths:a.$_ARRAY_ITEM_$.$_ARRAY_ITEM_$=OBJECT': 1,
      'paths:a.$_ARRAY_ITEM_$.$_ARRAY_ITEM_$.b=STRING': 1
    }
  )

  // Deeply nested arrays
  assert.same(
    walkRecord({ a: [{ b: [{ c: 'v' }] }] }, { inspectArrays: true }, { depth: 5 }).asObject(),
    {
      'paths:a=ARRAY': 1,
      'paths:a.$_ARRAY_ITEM_$=OBJECT': 1,
      'paths:a.$_ARRAY_ITEM_$.b=ARRAY': 1,
      'paths:a.$_ARRAY_ITEM_$.b.$_ARRAY_ITEM_$=OBJECT': 1,
      'paths:a.$_ARRAY_ITEM_$.b.$_ARRAY_ITEM_$.c=STRING': 1
    }
  )

  // Mixed types in the nested array
  assert.same(
    walkRecord({ a: [{ b: 'v' }, '', 0, true, null] }, { inspectArrays: true }, { depth: 5 }).asObject(),
    {
      'paths:a=ARRAY': 1,
      'paths:a.$_ARRAY_ITEM_$=OBJECT': 1,
      'paths:a.$_ARRAY_ITEM_$.b=STRING': 1,
      'paths:a.$_ARRAY_ITEM_$=STRING': 1,
      'paths:a.$_ARRAY_ITEM_$=NUMBER': 1,
      'paths:a.$_ARRAY_ITEM_$=BOOLEAN': 1,
      'paths:a.$_ARRAY_ITEM_$=NULL': 1
    }
  )

  // Overlapping paths
  assert.same(
    walkRecord({ a: [{ b: 'v' }, { b: 'v' }] }, { inspectArrays: true }, { depth: 5 }).asObject(),
    {
      'paths:a=ARRAY': 1,
      'paths:a.$_ARRAY_ITEM_$=OBJECT': 2,
      'paths:a.$_ARRAY_ITEM_$.b=STRING': 2
    }
  )
})
