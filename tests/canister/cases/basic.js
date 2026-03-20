import {
  assert,
  infoSql,
  createTableName,
  assertExecuteOk,
  createTableSql,
  insertSql,
  assertQueryOk,
} from '../harness.js';

async function testFreshInfo() {
  const databaseInfo = await infoSql();
  assert.deepEqual(databaseInfo.tables, []);
}

async function testInfoSorting() {
  const laterTable = createTableName('zeta');
  const earlierTable = createTableName('alpha');

  await assertExecuteOk(
    `CREATE TABLE ${laterTable} (id INTEGER)`,
    'create later table'
  );
  await assertExecuteOk(
    `CREATE TABLE ${earlierTable} (id INTEGER)`,
    'create earlier table'
  );

  const databaseInfo = await infoSql();
  const matchingTables = databaseInfo.tables.filter(
    (table) => table === earlierTable || table === laterTable
  );

  assert.deepEqual(matchingTables, [earlierTable, laterTable]);
}

async function testCreateTablesAndInsertRows() {
  const usersTable = createTableName('users');
  const ordersTable = createTableName('orders');
  const productsTable = createTableName('products');
  const usersColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'role', type: 'TEXT' },
    { name: 'city', type: 'TEXT' },
    { name: 'team', type: 'TEXT' },
    { name: 'level', type: 'INTEGER' },
    { name: 'score', type: 'REAL' },
    { name: 'email', type: 'TEXT' },
    { name: 'status', type: 'TEXT' },
    { name: 'note', type: 'TEXT' },
  ];
  const ordersColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'user_id', type: 'INTEGER' },
    { name: 'amount', type: 'REAL' },
    { name: 'currency', type: 'TEXT' },
    { name: 'region', type: 'TEXT' },
    { name: 'channel', type: 'TEXT' },
    { name: 'status', type: 'TEXT' },
    { name: 'remark', type: 'TEXT' },
    { name: 'created_day', type: 'TEXT' },
    { name: 'priority', type: 'INTEGER' },
  ];
  const productsColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'price', type: 'REAL' },
    { name: 'category', type: 'TEXT' },
    { name: 'brand', type: 'TEXT' },
    { name: 'sku', type: 'TEXT' },
    { name: 'stock', type: 'INTEGER' },
    { name: 'rating', type: 'REAL' },
    { name: 'shelf', type: 'TEXT' },
    { name: 'summary', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(usersTable, usersColumns),
    'create users table'
  );
  await assertExecuteOk(
    createTableSql(ordersTable, ordersColumns),
    'create orders table'
  );
  await assertExecuteOk(
    createTableSql(productsTable, productsColumns),
    'create products table'
  );

  for (let index = 1; index <= 100; index += 1) {
    await assertExecuteOk(
      insertSql(usersTable, usersColumns, [
        index,
        `Alice-${index}`,
        index % 2 === 0 ? 'admin' : 'member',
        `City-${index % 10}`,
        `Team-${index % 6}`,
        index,
        index + 0.5,
        `alice${index}@example.com`,
        index % 3 === 0 ? 'inactive' : 'active',
        `seed user ${index}`,
      ]),
      `insert user row ${index}`
    );

    await assertExecuteOk(
      insertSql(ordersTable, ordersColumns, [
        index,
        index,
        index * 10 + 0.25,
        'CNY',
        `REGION-${index % 5}`,
        index % 2 === 0 ? 'online' : 'offline',
        index % 4 === 0 ? 'pending' : 'paid',
        `priority order ${index}`,
        `2026-03-${String((index % 28) + 1).padStart(2, '0')}`,
        (index % 5) + 1,
      ]),
      `insert order row ${index}`
    );

    await assertExecuteOk(
      insertSql(productsTable, productsColumns, [
        index,
        `Keyboard-${index}`,
        index * 3 + 0.75,
        `category-${index % 4}`,
        `brand-${index % 7}`,
        `KB-${String(index).padStart(3, '0')}`,
        index * 2,
        (index % 5) + 0.8,
        `A-${String(index).padStart(2, '0')}`,
        `mechanical keyboard ${index}`,
      ]),
      `insert product row ${index}`
    );
  }

  const databaseInfo = await infoSql();
  assert.ok(databaseInfo.tables.includes(usersTable));
  assert.ok(databaseInfo.tables.includes(ordersTable));
  assert.ok(databaseInfo.tables.includes(productsTable));

  const userRows = await assertQueryOk(
    `SELECT id, name, role, city, team, level, score, email, status, note FROM ${usersTable} ORDER BY id`,
    'query users table'
  );
  assert.deepEqual(userRows.columns, ['id', 'name', 'role', 'city', 'team', 'level', 'score', 'email', 'status', 'note']);
  assert.equal(userRows.rows.length, 100);
  assert.deepEqual(userRows.rows[0], [
    { Integer: '1' },
    { Text: 'Alice-1' },
    { Text: 'member' },
    { Text: 'City-1' },
    { Text: 'Team-1' },
    { Integer: '1' },
    { Float: 1.5 },
    { Text: 'alice1@example.com' },
    { Text: 'active' },
    { Text: 'seed user 1' },
  ]);
  assert.deepEqual(userRows.rows[49], [
    { Integer: '50' },
    { Text: 'Alice-50' },
    { Text: 'admin' },
    { Text: 'City-0' },
    { Text: 'Team-2' },
    { Integer: '50' },
    { Float: 50.5 },
    { Text: 'alice50@example.com' },
    { Text: 'active' },
    { Text: 'seed user 50' },
  ]);
  assert.deepEqual(userRows.rows[99], [
    { Integer: '100' },
    { Text: 'Alice-100' },
    { Text: 'admin' },
    { Text: 'City-0' },
    { Text: 'Team-4' },
    { Integer: '100' },
    { Float: 100.5 },
    { Text: 'alice100@example.com' },
    { Text: 'active' },
    { Text: 'seed user 100' },
  ]);

  const orderRows = await assertQueryOk(
    `SELECT id, user_id, amount, currency, region, channel, status, remark, created_day, priority FROM ${ordersTable} ORDER BY id`,
    'query orders table'
  );
  assert.deepEqual(orderRows.columns, ['id', 'user_id', 'amount', 'currency', 'region', 'channel', 'status', 'remark', 'created_day', 'priority']);
  assert.equal(orderRows.rows.length, 100);
  assert.deepEqual(orderRows.rows[0], [
    { Integer: '1' },
    { Integer: '1' },
    { Float: 10.25 },
    { Text: 'CNY' },
    { Text: 'REGION-1' },
    { Text: 'offline' },
    { Text: 'paid' },
    { Text: 'priority order 1' },
    { Text: '2026-03-02' },
    { Integer: '2' },
  ]);
  assert.deepEqual(orderRows.rows[49], [
    { Integer: '50' },
    { Integer: '50' },
    { Float: 500.25 },
    { Text: 'CNY' },
    { Text: 'REGION-0' },
    { Text: 'online' },
    { Text: 'paid' },
    { Text: 'priority order 50' },
    { Text: '2026-03-23' },
    { Integer: '1' },
  ]);
  assert.deepEqual(orderRows.rows[99], [
    { Integer: '100' },
    { Integer: '100' },
    { Float: 1000.25 },
    { Text: 'CNY' },
    { Text: 'REGION-0' },
    { Text: 'online' },
    { Text: 'pending' },
    { Text: 'priority order 100' },
    { Text: '2026-03-17' },
    { Integer: '1' },
  ]);

  const productRows = await assertQueryOk(
    `SELECT id, name, price, category, brand, sku, stock, rating, shelf, summary FROM ${productsTable} ORDER BY id`,
    'query products table'
  );
  assert.deepEqual(productRows.columns, ['id', 'name', 'price', 'category', 'brand', 'sku', 'stock', 'rating', 'shelf', 'summary']);
  assert.equal(productRows.rows.length, 100);
  assert.deepEqual(productRows.rows[0], [
    { Integer: '1' },
    { Text: 'Keyboard-1' },
    { Float: 3.75 },
    { Text: 'category-1' },
    { Text: 'brand-1' },
    { Text: 'KB-001' },
    { Integer: '2' },
    { Float: 1.8 },
    { Text: 'A-01' },
    { Text: 'mechanical keyboard 1' },
  ]);
  assert.deepEqual(productRows.rows[49], [
    { Integer: '50' },
    { Text: 'Keyboard-50' },
    { Float: 150.75 },
    { Text: 'category-2' },
    { Text: 'brand-1' },
    { Text: 'KB-050' },
    { Integer: '100' },
    { Float: 0.8 },
    { Text: 'A-50' },
    { Text: 'mechanical keyboard 50' },
  ]);
  assert.deepEqual(productRows.rows[99], [
    { Integer: '100' },
    { Text: 'Keyboard-100' },
    { Float: 300.75 },
    { Text: 'category-0' },
    { Text: 'brand-2' },
    { Text: 'KB-100' },
    { Integer: '200' },
    { Float: 0.8 },
    { Text: 'A-100' },
    { Text: 'mechanical keyboard 100' },
  ]);
}

async function testTypedQueryResults() {
  const tableName = createTableName('records');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT, score REAL, note TEXT)`,
    'create records table'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, score, note) VALUES (1, 'Alice', 9.5, NULL)`,
    'insert first record'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, score, note) VALUES (2, 'Bob', 7.25, 'ready')`,
    'insert second record'
  );

  const result = await assertQueryOk(
    `SELECT id, name, score, note FROM ${tableName} ORDER BY id`,
    'select inserted records'
  );

  assert.deepEqual(result.columns, ['id', 'name', 'score', 'note']);
  assert.deepEqual(result.rows, [
    [
      { Integer: '1' },
      { Text: 'Alice' },
      { Float: 9.5 },
      { Null: null },
    ],
    [
      { Integer: '2' },
      { Text: 'Bob' },
      { Float: 7.25 },
      { Text: 'ready' },
    ],
  ]);
}

export const basicTestCases = [
  { fullName: 'testFreshInfo', displayName: 'fresh info is empty', fn: testFreshInfo },
  { fullName: 'testInfoSorting', displayName: 'info returns sorted tables', fn: testInfoSorting },
  { fullName: 'testCreateTablesAndInsertRows', displayName: 'create three tables and insert rows', fn: testCreateTablesAndInsertRows },
  { fullName: 'testTypedQueryResults', displayName: 'query returns typed values', fn: testTypedQueryResults },
];