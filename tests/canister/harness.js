import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const currentDir = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(currentDir, '../..');
let tableCounter = 0;

function runDfx(args, options = {}) {
  const output = execFileSync('dfx', args, {
    cwd: projectRoot,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    ...options,
  });

  if (typeof output !== 'string') {
    return '';
  }

  return output.trim();
}

function resetLocalReplica() {
  try {
    execFileSync('dfx', ['stop'], {
      cwd: projectRoot,
      stdio: 'ignore',
    });
  } catch {
    // Ignore stop failures when no local replica is running.
  }

  runDfx(['start', '--clean', '--background'], {
    stdio: 'inherit',
  });
}

function unwrapOk(result, action) {
  if ('Err' in result) {
    assert.fail(`${action} failed: ${result.Err}`);
  }

  return result.Ok;
}

function unwrapErr(result, action) {
  if ('Ok' in result) {
    assert.fail(`${action} unexpectedly succeeded`);
  }

  return result.Err;
}

function createTableName(prefix) {
  tableCounter += 1;
  return `${prefix}_${Date.now().toString(36)}_${tableCounter}`;
}

function sqlLiteral(value) {
  if (value === null) {
    return 'NULL';
  }

  if (typeof value === 'string') {
    return `'${value.replaceAll("'", "''")}'`;
  }

  return String(value);
}

function createTableSql(tableName, columns) {
  return `CREATE TABLE ${tableName} (${columns
    .map(({ name, type }) => `${name} ${type}`)
    .join(', ')})`;
}

function insertSql(tableName, columns, values) {
  return `INSERT INTO ${tableName} (${columns.map(({ name }) => name).join(', ')}) VALUES (${values
    .map(sqlLiteral)
    .join(', ')})`;
}

function callCanister(method, candidArgs = []) {
  const output = runDfx([
    'canister',
    'call',
    'backend',
    method,
    ...candidArgs,
    '--output',
    'json',
  ]);

  return JSON.parse(output);
}

function encodeTextVecCandid(values) {
  return `(vec { ${values.map((value) => JSON.stringify(value)).join('; ')} })`;
}

async function executeSql(sql, action = 'execute sql') {
  return callCanister('execute', [`(${JSON.stringify(sql)})`]);
}

async function batchExecuteSql(statements, action = 'execute batch sql') {
  return callCanister('execute_batch', [encodeTextVecCandid(statements)]);
}

async function querySql(sql, action = 'query sql') {
  return callCanister('query', [`(${JSON.stringify(sql)})`]);
}

async function infoSql() {
  return callCanister('info');
}

async function assertExecuteOk(sql, action) {
  const result = unwrapOk(await executeSql(sql, action), action);
  assert.equal(result.message, 'SQL statement executed successfully');
  return result;
}

async function assertBatchExecuteOk(statements, action) {
  return unwrapOk(await batchExecuteSql(statements, action), action);
}

async function assertQueryOk(sql, action) {
  return unwrapOk(await querySql(sql, action), action);
}

async function runCase(name, fn) {
  await fn();
  console.log(`PASS ${name}`);
}

function resolveSelectedTestCases(testCases, filters) {
  if (filters.length === 0) {
    return testCases;
  }

  const requestedNames = new Set(filters);
  const selected = testCases.filter((testCase) => requestedNames.has(testCase.fullName));

  if (selected.length !== requestedNames.size) {
    const knownNames = new Set(testCases.map((testCase) => testCase.fullName));
    const missing = [...requestedNames].filter((name) => !knownNames.has(name));
    throw new Error(
      `unknown test function name: ${missing.join(', ')}\navailable tests:\n${testCases
        .map((testCase) => `- ${testCase.fullName}`)
        .join('\n')}`
    );
  }

  return selected;
}

function parseRequestedTestNames(argv, testCases) {
  const args = argv.slice(2);

  if (args.includes('--list')) {
    for (const testCase of testCases) {
      console.log(`${testCase.fullName} - ${testCase.displayName}`);
    }
    process.exit(0);
  }

  return args.filter((arg) => !arg.startsWith('--'));
}

export {
  assert,
  runDfx,
  resetLocalReplica,
  unwrapOk,
  unwrapErr,
  createTableName,
  sqlLiteral,
  createTableSql,
  insertSql,
  callCanister,
  encodeTextVecCandid,
  executeSql,
  batchExecuteSql,
  querySql,
  infoSql,
  assertExecuteOk,
  assertBatchExecuteOk,
  assertQueryOk,
  runCase,
  resolveSelectedTestCases,
  parseRequestedTestNames,
};