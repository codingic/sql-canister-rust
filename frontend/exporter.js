import { utils, write } from 'xlsx';

const EXPORT_INSERT_BATCH_SIZE = 50;

function quoteIdentifier(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

function escapeSqlText(value) {
  return String(value).replaceAll("'", "''");
}

function chunkRows(rows, size) {
  const chunks = [];

  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }

  return chunks;
}

function normalizeIdentifier(value, fallback) {
  const text = String(value ?? '')
    .trim()
    .replace(/[\r\n\t]+/g, ' ')
    .replace(/\s+/g, '_');

  return text || fallback;
}

function ensureUniqueNames(values, fallbackPrefix) {
  const used = new Map();

  return values.map((value, index) => {
    const baseName = normalizeIdentifier(value, `${fallbackPrefix}_${index + 1}`);
    const currentCount = used.get(baseName) ?? 0;
    used.set(baseName, currentCount + 1);
    return currentCount === 0 ? baseName : `${baseName}_${currentCount + 1}`;
  });
}

function isNullCell(cell) {
  return !cell || (typeof cell === 'object' && 'Null' in cell);
}

function cellKind(cell) {
  if (isNullCell(cell)) {
    return 'Null';
  }

  if ('Integer' in cell) {
    return 'Integer';
  }

  if ('Float' in cell) {
    return 'Float';
  }

  if ('Text' in cell) {
    return 'Text';
  }

  if ('Blob' in cell) {
    return 'Blob';
  }

  return 'Text';
}

function inferColumnType(rows, columnIndex) {
  const kinds = rows
    .map((row) => cellKind(row[columnIndex]))
    .filter((kind) => kind !== 'Null');

  if (kinds.length === 0) {
    return 'TEXT';
  }

  if (kinds.includes('Text')) {
    return 'TEXT';
  }

  if (kinds.includes('Blob')) {
    return 'BLOB';
  }

  if (kinds.includes('Float')) {
    return 'REAL';
  }

  if (kinds.every((kind) => kind === 'Integer')) {
    return 'INTEGER';
  }

  return 'TEXT';
}

function toHex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('').toUpperCase();
}

function sqlLiteral(cell) {
  const kind = cellKind(cell);

  if (kind === 'Null') {
    return 'NULL';
  }

  if (kind === 'Integer') {
    return String(cell.Integer);
  }

  if (kind === 'Float') {
    return String(cell.Float);
  }

  if (kind === 'Blob') {
    return `X'${toHex(cell.Blob)}'`;
  }

  return `'${escapeSqlText(cell.Text)}'`;
}

function workbookValue(cell) {
  const kind = cellKind(cell);

  if (kind === 'Null') {
    return null;
  }

  if (kind === 'Integer') {
    return cell.Integer;
  }

  if (kind === 'Float') {
    return cell.Float;
  }

  if (kind === 'Blob') {
    return `X'${toHex(cell.Blob)}'`;
  }

  return cell.Text;
}

function normalizeSheetName(name) {
  const normalized = String(name ?? '')
    .replace(/[\\/?*\[\]:]/g, '_')
    .trim();

  return (normalized || 'query_result').slice(0, 31);
}

export function normalizeExportName(name, fallback = 'query_result') {
  const normalized = String(name ?? '')
    .trim()
    .replace(/[\\/:*?"<>|]+/g, '_')
    .replace(/\s+/g, '_');

  return normalized || fallback;
}

export function queryResultToSqlText(tableName, columns, rows) {
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new Error('没有可导出的列');
  }

  const safeTableName = normalizeIdentifier(tableName, 'query_result');
  const safeColumns = ensureUniqueNames(columns, 'column');
  const createSql = `CREATE TABLE ${quoteIdentifier(safeTableName)} (${safeColumns
    .map((column, index) => `${quoteIdentifier(column)} ${inferColumnType(rows, index)}`)
    .join(', ')})`;

  const insertSql = chunkRows(rows, EXPORT_INSERT_BATCH_SIZE).map((rowBatch) => {
    const valuesList = rowBatch.map((row) => {
      const values = safeColumns.map((_, index) => sqlLiteral(row[index]));
      return `(${values.join(', ')})`;
    });

    return `INSERT INTO ${quoteIdentifier(safeTableName)} (${safeColumns
      .map(quoteIdentifier)
      .join(', ')}) VALUES ${valuesList.join(', ')}`;
  });

  return ['BEGIN', createSql, ...insertSql, 'COMMIT'].join(';\n') + ';';
}

export function queryResultToWorkbook(tableName, columns, rows) {
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new Error('没有可导出的列');
  }

  const data = [columns, ...rows.map((row) => columns.map((_, index) => workbookValue(row[index])))];
  const workbook = utils.book_new();
  const worksheet = utils.aoa_to_sheet(data);

  utils.book_append_sheet(workbook, worksheet, normalizeSheetName(tableName));
  return workbook;
}

export function queryResultToXlsxBytes(tableName, columns, rows) {
  const workbook = queryResultToWorkbook(tableName, columns, rows);
  return write(workbook, { type: 'array', bookType: 'xlsx' });
}