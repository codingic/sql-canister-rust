import {
  assert,
  createTableName,
  assertExecuteOk,
  createTableSql,
  insertSql,
  assertQueryOk,
} from '../harness.js';

async function testAdvancedSelectFeatures() {
  const tableName = createTableName('advanced_select');
  const columns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'category', type: 'TEXT' },
    { name: 'score', type: 'REAL' },
    { name: 'note', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(tableName, columns),
    'create advanced select table'
  );

  const seedRows = [
    [1, 'alpha', 10.0, null],
    [2, 'alpha', 15.0, 'eligible'],
    [3, 'beta', 22.5, 'beta-only'],
    [4, 'alpha', 25.0, 'priority'],
    [5, 'gamma', 30.0, null],
    [6, 'alpha', 28.0, 'latest'],
  ];

  for (const row of seedRows) {
    await assertExecuteOk(
      insertSql(tableName, columns, row),
      `insert advanced select row ${row[0]}`
    );
  }

  const distinctResult = await assertQueryOk(
    `SELECT DISTINCT category FROM ${tableName} ORDER BY category DESC LIMIT 2 OFFSET 1`,
    'query distinct categories with paging'
  );
  assert.deepEqual(distinctResult.columns, ['category']);
  assert.deepEqual(distinctResult.rows, [
    [{ Text: 'beta' }],
    [{ Text: 'alpha' }],
  ]);

  const filteredResult = await assertQueryOk(
    `SELECT id, score / 2 AS half_score FROM ${tableName} WHERE id IN (1, 2, 4, 6) AND score BETWEEN 10 AND 30 AND note IS NOT NULL AND category LIKE 'a%' ORDER BY score DESC LIMIT 2`,
    'query advanced filters and decimal division'
  );
  assert.deepEqual(filteredResult.columns, ['id', 'half_score']);
  assert.deepEqual(filteredResult.rows, [
    [
      { Integer: '6' },
      { Float: 14 },
    ],
    [
      { Integer: '4' },
      { Float: 12.5 },
    ],
  ]);

  const nullResult = await assertQueryOk(
    `SELECT id FROM ${tableName} WHERE note IS NULL ORDER BY id`,
    'query null predicate'
  );
  assert.deepEqual(nullResult.rows, [
    [{ Integer: '1' }],
    [{ Integer: '5' }],
  ]);

  const noFromResult = await assertQueryOk(
    'SELECT 1 / 2 AS half, 5 / 2 AS two_point_five, 3.0 / 2 AS one_point_five',
    'query constant division'
  );
  assert.deepEqual(noFromResult.columns, ['half', 'two_point_five', 'one_point_five']);
  assert.deepEqual(noFromResult.rows, [[
    { Float: 0.5 },
    { Float: 2.5 },
    { Float: 1.5 },
  ]]);
}

async function testGroupedSelectFeatures() {
  const tableName = createTableName('grouped_select');
  const columns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'category', type: 'TEXT' },
    { name: 'score', type: 'REAL' },
    { name: 'note', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(tableName, columns),
    'create grouped select table'
  );

  const seedRows = [
    [1, 'alpha', 10.0, null],
    [2, 'alpha', 15.0, 'eligible'],
    [3, 'beta', 22.5, 'beta-only'],
    [4, 'alpha', 25.0, 'priority'],
    [5, 'gamma', 30.0, null],
    [6, 'alpha', 28.0, 'latest'],
  ];

  for (const row of seedRows) {
    await assertExecuteOk(
      insertSql(tableName, columns, row),
      `insert grouped select row ${row[0]}`
    );
  }

  const groupedResult = await assertQueryOk(
    `SELECT category, COUNT(*) AS item_count, SUM(score) AS total_score, AVG(score) AS average_score FROM ${tableName} GROUP BY category HAVING SUM(score) >= 20 ORDER BY SUM(score) DESC LIMIT 2`,
    'query grouped aggregates with having and limit'
  );
  assert.deepEqual(groupedResult.columns, ['category', 'item_count', 'total_score', 'average_score']);
  assert.deepEqual(groupedResult.rows, [
    [
      { Text: 'alpha' },
      { Integer: '4' },
      { Float: 78 },
      { Float: 19.5 },
    ],
    [
      { Text: 'gamma' },
      { Integer: '1' },
      { Float: 30 },
      { Float: 30 },
    ],
  ]);

  const groupedFilterResult = await assertQueryOk(
    `SELECT category FROM ${tableName} GROUP BY category HAVING category LIKE 'a%' ORDER BY category`,
    'query grouped rows without aggregate select columns'
  );
  assert.deepEqual(groupedFilterResult.columns, ['category']);
  assert.deepEqual(groupedFilterResult.rows, [
    [{ Text: 'alpha' }],
  ]);
}

export const selectFeatureTestCases = [
  { fullName: 'testAdvancedSelectFeatures', displayName: 'advanced select features work together', fn: testAdvancedSelectFeatures },
  { fullName: 'testGroupedSelectFeatures', displayName: 'group by and having aggregates work together', fn: testGroupedSelectFeatures },
];