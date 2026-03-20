import {
  assert,
  assertQueryOk,
  unwrapErr,
  querySql,
} from '../harness.js';

async function testCompoundSelectFeatures() {
  const unionResult = await assertQueryOk(
    'SELECT 1 AS value UNION SELECT 1 AS value UNION SELECT 2 AS value',
    'query union distinct constants'
  );
  assert.deepEqual(unionResult.columns, ['value']);
  assert.deepEqual(unionResult.rows, [
    [{ Integer: '1' }],
    [{ Integer: '2' }],
  ]);

  const unionAllResult = await assertQueryOk(
    'SELECT 1 AS value UNION ALL SELECT 1 AS value UNION ALL SELECT 2 AS value',
    'query union all constants'
  );
  assert.deepEqual(unionAllResult.columns, ['value']);
  assert.deepEqual(unionAllResult.rows, [
    [{ Integer: '1' }],
    [{ Integer: '1' }],
    [{ Integer: '2' }],
  ]);

  const intersectResult = await assertQueryOk(
    'SELECT 1 AS value UNION ALL SELECT 2 AS value INTERSECT SELECT 2 AS value',
    'query intersect constants'
  );
  assert.deepEqual(intersectResult.columns, ['value']);
  assert.deepEqual(intersectResult.rows, [
    [{ Integer: '2' }],
  ]);

  const exceptResult = await assertQueryOk(
    'SELECT 1 AS value UNION ALL SELECT 2 AS value EXCEPT SELECT 1 AS value',
    'query except constants'
  );
  assert.deepEqual(exceptResult.columns, ['value']);
  assert.deepEqual(exceptResult.rows, [
    [{ Integer: '2' }],
  ]);

  const orderedCompoundResult = await assertQueryOk(
    'SELECT 1 AS value UNION ALL SELECT 3 AS value UNION ALL SELECT 2 AS value ORDER BY value DESC LIMIT 2 OFFSET 1',
    'query compound order by with limit and offset'
  );
  assert.deepEqual(orderedCompoundResult.columns, ['value']);
  assert.deepEqual(orderedCompoundResult.rows, [
    [{ Integer: '2' }],
    [{ Integer: '1' }],
  ]);
}

async function testCompoundSelectErrorHandling() {
  const mismatchedColumnsErr = unwrapErr(
    await querySql(
      'SELECT 1 AS value UNION SELECT 1 AS value, 2 AS other',
      'query compound select with mismatched column counts'
    ),
    'query compound select with mismatched column counts'
  );
  assert.ok(mismatchedColumnsErr.includes('compound query do not have the same number of result columns'));

  const intersectAllErr = unwrapErr(
    await querySql(
      'SELECT 1 AS value INTERSECT ALL SELECT 1 AS value',
      'query unsupported intersect all'
    ),
    'query unsupported intersect all'
  );
  assert.ok(intersectAllErr.includes('INTERSECT ALL is not currently supported'));

  const exceptAllErr = unwrapErr(
    await querySql(
      'SELECT 1 AS value EXCEPT ALL SELECT 1 AS value',
      'query unsupported except all'
    ),
    'query unsupported except all'
  );
  assert.ok(exceptAllErr.includes('EXCEPT ALL is not currently supported'));

  const trailingTokenErr = unwrapErr(
    await querySql(
      'SELECT 1 value extra',
      'query select with trailing token'
    ),
    'query select with trailing token'
  );
  assert.ok(trailingTokenErr.includes('Unexpected trailing token: extra'));
}

export const compoundFeatureTestCases = [
  { fullName: 'testCompoundSelectFeatures', displayName: 'compound select set operators and outer ordering work', fn: testCompoundSelectFeatures },
  { fullName: 'testCompoundSelectErrorHandling', displayName: 'compound select errors surface clearly', fn: testCompoundSelectErrorHandling },
];