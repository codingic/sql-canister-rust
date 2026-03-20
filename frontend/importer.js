import { read, utils } from 'xlsx';

const INSERT_BATCH_SIZE = 50;

function quoteIdentifier(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

function escapeSqlText(value) {
  return String(value).replaceAll("'", "''");
}

function fileStem(fileName) {
  return String(fileName).replace(/\.[^.]+$/, '') || 'imported_data';
}

function normalizeIdentifier(value, fallback) {
  const text = String(value ?? '')
    .trim()
    .replace(/[\r\n\t]+/g, ' ')
    .replace(/\s+/g, '_');

  if (!text) {
    return fallback;
  }

  return text;
}

function isIntegerLike(value) {
  return /^-?\d+$/.test(String(value));
}

function isNumberLike(value) {
  return /^-?(?:\d+|\d+\.\d+|\d+\.\d+e[+-]?\d+|\d+e[+-]?\d+)$/i.test(String(value));
}

function inferColumnType(values) {
  const filled = values.filter((value) => value !== null && value !== undefined && value !== '');

  if (filled.length === 0) {
    return 'TEXT';
  }

  if (filled.every((value) => typeof value === 'number' && Number.isInteger(value))) {
    return 'INTEGER';
  }

  if (filled.every((value) => typeof value === 'number')) {
    return 'REAL';
  }

  if (filled.every((value) => isIntegerLike(value))) {
    return 'INTEGER';
  }

  if (filled.every((value) => isNumberLike(value))) {
    return 'REAL';
  }

  return 'TEXT';
}

function toSqlLiteral(value, type) {
  if (value === null || value === undefined || value === '') {
    return 'NULL';
  }

  if (type === 'INTEGER' || type === 'REAL') {
    return String(value);
  }

  return `'${escapeSqlText(value)}'`;
}

function chunkRows(rows, size) {
  const chunks = [];

  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }

  return chunks;
}

function worksheetToTableSql(sheetName, rows) {
  if (rows.length === 0) {
    throw new Error(`工作表 ${sheetName} 没有可导入的数据`);
  }

  const [headerRow, ...bodyRows] = rows;
  if (!headerRow || headerRow.length === 0) {
    throw new Error(`工作表 ${sheetName} 缺少表头`);
  }

  const tableName = normalizeIdentifier(sheetName, 'sheet');
  const columns = headerRow.map((header, index) => normalizeIdentifier(header, `column_${index + 1}`));
  const uniqueColumns = columns.map((column, index) => {
    const firstIndex = columns.indexOf(column);
    return firstIndex === index ? column : `${column}_${index + 1}`;
  });

  const columnTypes = uniqueColumns.map((_, columnIndex) =>
    inferColumnType(bodyRows.map((row) => row[columnIndex]))
  );

  const createSql = `CREATE TABLE ${quoteIdentifier(tableName)} (${uniqueColumns
    .map((column, index) => `${quoteIdentifier(column)} ${columnTypes[index]}`)
    .join(', ')})`;

  const nonEmptyRows = bodyRows
    .filter((row) => row.some((cell) => cell !== null && cell !== undefined && cell !== ''))
  const insertSql = chunkRows(nonEmptyRows, INSERT_BATCH_SIZE).map((rowBatch) => {
    const valuesList = rowBatch.map((row) => {
      const values = uniqueColumns.map((_, index) => toSqlLiteral(row[index], columnTypes[index]));
      return `(${values.join(', ')})`;
    });

    return `INSERT INTO ${quoteIdentifier(tableName)} (${uniqueColumns
      .map(quoteIdentifier)
      .join(', ')}) VALUES ${valuesList.join(', ')}`;
  });

  return ['BEGIN', createSql, ...insertSql, 'COMMIT'].join(';\n') + ';';
}

export function workbookToSqlText(workbook, fileName = 'import.xlsx') {
  const parts = workbook.SheetNames.map((sheetName) => {
    const worksheet = workbook.Sheets[sheetName];
    const rows = utils.sheet_to_json(worksheet, {
      header: 1,
      defval: null,
      raw: true,
    });

    return worksheetToTableSql(sheetName || fileStem(fileName), rows);
  });

  return parts.join('\n\n');
}

export async function readImportFile(file) {
  const lowerName = String(file.name || '').toLowerCase();

  if (lowerName.endsWith('.sql')) {
    return {
      kind: 'sql',
      sqlText: await file.text(),
      sourceName: file.name,
    };
  }

  if (lowerName.endsWith('.xlsx')) {
    const buffer = await file.arrayBuffer();
    const workbook = read(buffer, { type: 'array' });

    return {
      kind: 'xlsx',
      sqlText: workbookToSqlText(workbook, file.name),
      sourceName: file.name,
      sheetCount: workbook.SheetNames.length,
    };
  }

  throw new Error('仅支持导入 .sql 和 .xlsx 文件');
}