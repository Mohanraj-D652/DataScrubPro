// ══════════════════════════════════════════════════════════════════════════
// DATASCRUB PRO - CLEANING ENGINE v4.3
// Fixed: Phone transform column-name guard (no more corrupted description/text cols)
// Added: Column-selector downloads (CSV / JSON / SQL for chosen columns only)
// ══════════════════════════════════════════════════════════════════════════

export interface CleaningConfig {
  tableName: string;
  pkColumn: string;
  eol: string;
  encoding: string;
  mode: 'standard' | 'advanced';
  removeDuplicates: boolean;
  removeEmpty: boolean;
  trimWhitespace: boolean;
  normalizeValues: boolean;
  fixEncoding: boolean;
  fuzzyDuplicates: boolean;
  validateEmail: boolean;
  standardizePhone: boolean;
  normalizeCase: boolean;
  standardizeDate: boolean;
  detectOutliers: boolean;
  removeSpecialChars: boolean;
  crossFieldValidation: boolean;
  fillMissing: boolean;
  standardizeAddress: boolean;
  removeHtmlTags: boolean;
  fixNumberFormats: boolean;
  generateId: boolean;
  removeRowsWithEmptyValues: boolean;
}

export interface CleaningStats {
  original: number;
  cleaned: number;
  removed: number;
  cols: number;
  fixed: number;
}

export interface LogEntry {
  icon: string;
  message: string;
  type: '' | 'success' | 'warn' | 'error';
}

export type ColumnTypes = Record<string, string>;

// ══════════════════════════════════════════════════════════════════════════
// MODULE-LEVEL STORE
// ══════════════════════════════════════════════════════════════════════════
let _storedChunks: string[] = [];
let _storedHeaders: string[] = [];
let _storedFileName = '';
let _storedSeparator = ',';
let _storedCreateSQL = '';
let _storedLoadSQL = '';

export function storeResult(data: {
  chunks: string[];
  headers: string[];
  fileName: string;
  createSQL: string;
  loadSQL: string;
  separator?: string;
}) {
  _storedChunks = data.chunks;
  _storedHeaders = data.headers;
  _storedFileName = data.fileName;
  _storedCreateSQL = data.createSQL;
  _storedLoadSQL = data.loadSQL;
  _storedSeparator = data.separator || ',';
}

// ══════════════════════════════════════════════════════════════════════════
// DOWNLOAD — uses Blob URLs for fast, non-blocking downloads
// ══════════════════════════════════════════════════════════════════════════

function triggerBlobDownload(content: string, filename: string, mimeType: string): boolean {
  try {
    if (mimeType.includes('json')) {
      mimeType = 'application/json;charset=utf-8';
    }
    const blob = new Blob([content], { type: mimeType });
    console.log('Blob created:', blob.size, 'bytes, type:', mimeType);
    const url = URL.createObjectURL(blob);
    console.log('Object URL created:', url);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.style.display = 'none';
    document.body.appendChild(link);
    link.click();
    console.log('Download triggered for:', filename);
    document.body.removeChild(link);
    setTimeout(() => {
      URL.revokeObjectURL(url);
      console.log('Object URL revoked');
    }, 500);
    return true;
  } catch (err) {
    console.error('Blob download failed:', err);
    return false;
  }
}

export function downloadCSV(selectedColumns?: string[]): boolean {
  if (!_storedChunks || _storedChunks.length === 0) {
    alert('No cleaned data available. Please clean a file first.');
    return false;
  }
  
  // If specific columns are selected, filter the data
  if (selectedColumns && selectedColumns.length > 0) {
    const colIndices = selectedColumns.map(col => _storedHeaders.indexOf(col)).filter(idx => idx !== -1);
    if (colIndices.length === 0) {
      alert('Selected columns not found.');
      return false;
    }
    
    // Filter headers
    const filteredHeaders = colIndices.map(idx => _storedHeaders[idx]);
    const headerLine = filteredHeaders.map(h => quoteCell(h)).join(_storedSeparator) + '\n';
    
    // Filter data rows
    const allRows = getAllDataRows(_storedChunks, undefined, _storedSeparator);
    const filteredRows = allRows.map(row => 
      colIndices.map(idx => quoteCell(row[idx] || '')).join(_storedSeparator)
    ).join('\n');
    
    const content = headerLine + filteredRows;
    const name = (_storedFileName || 'data').replace(/\.[^.]+$/, '') + '_selected.csv';
    return triggerBlobDownload(content, name, 'text/csv;charset=utf-8');
  }
  
  const content = _storedChunks.join('');
  const name = (_storedFileName || 'data').replace(/\.[^.]+$/, '') + '_cleaned.csv';
  return triggerBlobDownload(content, name, 'text/csv;charset=utf-8');
}

export function downloadSQL(selectedColumns?: string[]): boolean {
  if (!_storedCreateSQL || !_storedLoadSQL) {
    alert('No SQL available. Please clean a file first.');
    return false;
  }
  try {
    let createSQL = _storedCreateSQL;
    let loadSQL = _storedLoadSQL;
    
    // If specific columns are selected, regenerate SQL for those columns only
    if (selectedColumns && selectedColumns.length > 0) {
      const validColumns = selectedColumns.filter(col => _storedHeaders.includes(col));
      if (validColumns.length === 0) {
        alert('No valid columns selected.');
        return false;
      }
      
      // This is a simplified version - removes columns from CREATE and LOAD statements
      // For production, you might want more sophisticated SQL generation
      const columnsClause = validColumns.join(', ');
      createSQL = createSQL.replace(/\([^)]+\)/, `(\n  ${validColumns.map(col => col + ' TEXT').join(',\n  ')}\n)`);
      loadSQL = loadSQL.replace(/LOAD DATA[^(]*\(/, `LOAD DATA ... (${columnsClause}`) + ' ...';
    }
    
    const content = createSQL + '\n\n' + loadSQL;
    const fileName = (_storedFileName || 'data').replace(/\.[^.]+$/, '') + (selectedColumns && selectedColumns.length > 0 ? '_selected' : '') + '_import.sql';
    return triggerBlobDownload(content, fileName, 'text/plain;charset=utf-8');
  } catch (err) {
    console.error('SQL download error:', err);
    return false;
  }
}

export async function downloadJSON(selectedColumns?: string[]): Promise<boolean> {
  if (!_storedChunks || _storedChunks.length === 0) {
    console.error('No chunks stored');
    alert('No data available. Please clean a file first.');
    return false;
  }
  if (!_storedHeaders || _storedHeaders.length === 0) {
    console.error('No headers stored');
    alert('No headers available. Please clean a file first.');
    return false;
  }
  try {
    await new Promise(resolve => setTimeout(resolve, 100));
    const allRows = getAllDataRows(_storedChunks, undefined, _storedSeparator);
    console.log('Retrieved rows for JSON:', allRows.length);
    if (!allRows || allRows.length === 0) {
      console.error('No rows found');
      alert('No data rows found.');
      return false;
    }
    
    // Determine which columns to include
    const headersToUse = selectedColumns && selectedColumns.length > 0 
      ? selectedColumns.filter(col => _storedHeaders.includes(col))
      : _storedHeaders;
    
    if (headersToUse.length === 0) {
      alert('No valid columns selected.');
      return false;
    }
    
    const jsonData: Record<string, string>[] = [];
    const chunkSize = 5000;
    for (let i = 0; i < allRows.length; i += chunkSize) {
      const endIdx = Math.min(i + chunkSize, allRows.length);
      for (let j = i; j < endIdx; j++) {
        const cells = allRows[j];
        if (!cells || cells.length === 0) continue; // Skip empty rows
        const row: Record<string, string> = {};
        // Ensure we have the correct number of cells, pad if needed
        const paddedCells = [...cells];
        while (paddedCells.length < _storedHeaders.length) {
          paddedCells.push('');
        }
        // Map each cell to its corresponding header
        for (const header of headersToUse) {
          const headerIdx = _storedHeaders.indexOf(header);
          if (headerIdx !== -1) {
            const value = paddedCells[headerIdx] ? String(paddedCells[headerIdx]).trim() : '';
            row[header] = value;
          }
        }
        jsonData.push(row);
      }
      if (endIdx < allRows.length) {
        await new Promise(resolve => setTimeout(resolve, 5));
      }
    }
    if (jsonData.length === 0) {
      alert('Failed to create JSON data.');
      return false;
    }
    const jsonContent = JSON.stringify(jsonData, null, 2);
    const fileName = (_storedFileName || 'data').replace(/\.[^\.]+$/, '') + (selectedColumns && selectedColumns.length > 0 ? '_selected' : '') + '_cleaned.json';
    const success = triggerBlobDownload(jsonContent, fileName, 'application/json;charset=utf-8');
    console.log('JSON download triggered:', success);
    return success;
  } catch (err) {
    console.error('JSON download error:', err);
    alert('Error: ' + (err instanceof Error ? err.message : 'Failed to download JSON'));
    return false;
  }
}

// ── CSV PARSER ──────────────────────────────────────────────────────────

export function parseCSVLine(line: string, sep: string): string[] {
  const cells: string[] = [];
  let cell = '';
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        cell += '"';
        i += 2;
        continue;
      }
      if (ch === '"') {
        inQuotes = false;
        i++;
        continue;
      }
      cell += ch;
    } else {
      if (ch === '"') {
        inQuotes = true;
        i++;
        continue;
      }
      if (ch === sep) {
        let trimmed = cell.trim();
        if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
          trimmed = trimmed.slice(1, -1);
        }
        cells.push(trimmed);
        cell = '';
        i++;
        continue;
      }
      cell += ch;
    }
    i++;
  }
  let lastTrimmed = cell.trim();
  if (lastTrimmed.startsWith('"') && lastTrimmed.endsWith('"')) {
    lastTrimmed = lastTrimmed.slice(1, -1);
  }
  cells.push(lastTrimmed);
  return cells;
}

export function detectSeparator(line: string): string {
  const counts: Record<string, number> = {
    ',': (line.match(/,/g) || []).length,
    '\t': (line.match(/\t/g) || []).length,
    ';': (line.match(/;/g) || []).length,
    '|': (line.match(/\|/g) || []).length,
  };
  return Object.entries(counts).sort((a, b) => b[1] - a[1])[0][0];
}

export function quoteCell(value: string | null | undefined): string {
  if (value === null || value === undefined) return '';
  const str = String(value);
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

export function sanitizeColumnName(name: string): string {
  return String(name || '')
    .trim()
    .replace(/^\uFEFF/, '')
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_')
    .toLowerCase() || 'col_' + Math.random().toString(36).substring(2, 7);
}

// ── ADVANCED CLEANING ─────────────────────────────────────────────────────

export function looksLikeEmail(str: string): boolean {
  return !!str && /@/.test(str) && str.length > 5;
}

export function fixEmail(email: string): string {
  if (!email) return email;
  let fixed = email.toLowerCase().trim().replace(/\s+/g, '').replace(/,,+/g, ',').replace(/\.+/g, '.');
  const domainFixes: [string, string][] = [
    ['gmial.com', 'gmail.com'], ['gmai.com', 'gmail.com'],
    ['gamil.com', 'gmail.com'], ['gnail.com', 'gmail.com'],
    ['gmail.co', 'gmail.com'], ['gmail.con', 'gmail.com'],
    ['yahooo.com', 'yahoo.com'], ['yahoo.con', 'yahoo.com'],
    ['hotmial.com', 'hotmail.com'], ['hotmail.con', 'hotmail.com'],
    ['outlok.com', 'outlook.com'], ['outlook.con', 'outlook.com'],
  ];
  for (const [typo, correct] of domainFixes) {
    if (fixed.endsWith('@' + typo)) {
      fixed = fixed.replace('@' + typo, '@' + correct);
      break;
    }
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(fixed)) return email;
  return fixed;
}

export function looksLikePhone(str: string): boolean {
  if (!str) return false;
  const trimmed = str.trim();
  // ── FIX: Must look *exclusively* like a phone number.
  // The old regex matched any string with 7-15 digits + separators, which
  // caught sentences, IDs, descriptions, season counts, etc.
  // New rule: after stripping all allowed phone characters the remainder must
  // be empty — i.e. the cell contains NOTHING but digits and phone punctuation.
  const stripped = trimmed.replace(/[\d\s\+\-\.\(\)]/g, '');
  if (stripped.length > 0) return false; // has letters/other chars → not a phone
  const digits = trimmed.replace(/\D/g, '');
  return digits.length >= 7 && digits.length <= 15;
}

// Returns true when a column name suggests it stores phone numbers.
// Used to gate the standardizePhone transform so it never fires on
// description / text / season / rating columns etc.
export function isPhoneColumnName(colName: string): boolean {
  return /phone|mobile|cell|fax|tel|contact_no|contact_num|whatsapp/i.test(colName);
}

export function standardizePhoneFn(phone: string): string {
  if (!phone) return phone;
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 10) return `(${digits.substring(0,3)}) ${digits.substring(3,6)}-${digits.substring(6)}`;
  if (digits.length === 11 && digits[0] === '1') return `+1 (${digits.substring(1,4)}) ${digits.substring(4,7)}-${digits.substring(7)}`;
  if (digits.length >= 11) {
    const cc = digits.substring(0, digits.length - 10);
    const rest = digits.substring(digits.length - 10);
    return `+${cc} (${rest.substring(0,3)}) ${rest.substring(3,6)}-${rest.substring(6)}`;
  }
  return phone;
}

export function isValidDate(str: string): boolean {
  if (!str || str.length < 6 || str.length > 30) return false;
  if (/^\d+$/.test(str) && str.length > 8) return false;
  const patterns = [
    /^\d{4}-\d{1,2}-\d{1,2}$/, /^\d{1,2}\/\d{1,2}\/\d{4}$/,
    /^\d{1,2}-\d{1,2}-\d{4}$/, /^\d{4}\/\d{1,2}\/\d{1,2}$/,
    /^\d{1,2}\.\d{1,2}\.\d{4}$/, /^\d{4}\.\d{1,2}\.\d{1,2}$/,
    /^[A-Za-z]+ \d{1,2},? \d{4}$/, /^\d{1,2} [A-Za-z]+ \d{4}$/,
  ];
  for (const pattern of patterns) {
    if (pattern.test(str.trim())) {
      const date = new Date(str);
      if (!isNaN(date.getTime())) {
        const year = date.getFullYear();
        return year >= 1900 && year <= 2100;
      }
    }
  }
  return false;
}

export function standardizeDateFn(dateStr: string): string {
  if (!dateStr) return dateStr;
  try {
    const ddmmyyyy = dateStr.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{4})$/);
    if (ddmmyyyy) {
      const [, d, m, y] = ddmmyyyy;
      const day = parseInt(d, 10);
      const month = parseInt(m, 10);
      if (day > 12 && month <= 12) {
        return `${y}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
      }
    }
    const date = new Date(dateStr);
    if (!isNaN(date.getTime())) {
      const year = date.getFullYear();
      if (year >= 1900 && year <= 2100) {
        return `${year}-${String(date.getMonth()+1).padStart(2,'0')}-${String(date.getDate()).padStart(2,'0')}`;
      }
    }
  } catch { /* noop */ }
  return dateStr;
}

export function normalizeCaseFn(str: string, columnName: string): string {
  if (!str || str.length === 0) return str;
  const lower = columnName.toLowerCase();
  if (/email|e_mail/.test(lower)) return str.toLowerCase();
  if (/url|website|href|link/.test(lower)) return str.toLowerCase();
  if (/^(first|last|middle|full)?_?name$|^city$|^country$|^state$|^street$|^address$/i.test(lower)) {
    return str.split(/(\s+)/).map(word => {
      if (word.trim() === '') return word;
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }).join('');
  }
  return str;
}

export function removeSpecialCharsFn(str: string): string {
  if (!str) return str;
  return str.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');
}

export function removeHtmlTagsFn(str: string): string {
  if (!str) return str;
  return str
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<').replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"').replace(/&#039;/gi, "'");
}

export function fixNumberFormat(str: string): string {
  if (!str) return str;
  const cleaned = str.replace(/[$\u20AC\u00A3\u00A5\u20B9]/g, '').trim();
  if (/^\d{1,3}(,\d{3})+(\.\d+)?$/.test(cleaned)) return cleaned.replace(/,/g, '');
  return str;
}

export function fixEncodingIssues(str: string): string {
  if (!str) return str;
  const fixes: [string, string][] = [
    ['\u00C3\u00A9', '\u00E9'], ['\u00C3\u00A8', '\u00E8'],
    ['\u00C3\u00A0', '\u00E0'], ['\u00C3\u00A2', '\u00E2'],
    ['\u00C3\u00AE', '\u00EE'], ['\u00C3\u00B4', '\u00F4'],
    ['\u00C3\u00BB', '\u00FB'], ['\u00C3\u00A7', '\u00E7'],
    ['\u00C3\u00BC', '\u00FC'], ['\u00C3\u00B6', '\u00F6'],
    ['\u00C3\u00A4', '\u00E4'], ['\u00C3\u00B1', '\u00F1'],
    ['\u00E2\u0080\u0099', "'"], ['\u00E2\u0080\u009C', '"'],
    ['\u00E2\u0080\u009D', '"'], ['\u00E2\u0080\u0094', '\u2014'],
    ['\u00E2\u0080\u0093', '\u2013'], ['\u00E2\u0080\u00A6', '\u2026'],
    ['\u00C2\u00A0', ' '], ['\u00C2\u00B0', '\u00B0'],
  ];
  let result = str;
  for (const [bad, good] of fixes) {
    if (result.includes(bad)) result = result.split(bad).join(good);
  }
  return result.replace(/^\uFEFF/, '');
}

// ── TYPE DETECTION ────────────────────────────────────────────────────────

export function detectColumnTypes(headers: string[], sampleRows: string[][]): ColumnTypes {
  const types: ColumnTypes = {};
  headers.forEach((header, colIndex) => {
    const values = sampleRows
      .map(row => String(row[colIndex] || '').trim())
      .filter(v => v && !/^(null|NULL|N\/A|na|none|undefined|nil|\?|-)$/i.test(v));

    if (values.length === 0) { types[header] = 'varchar'; return; }

    if (values.every(v => /^-?\d+$/.test(v))) {
      const maxVal = Math.max(...values.map(v => Math.abs(parseInt(v, 10))));
      if (maxVal < 128) types[header] = 'tinyint';
      else if (maxVal < 32768) types[header] = 'smallint';
      else if (maxVal < 2147483648) types[header] = 'int';
      else types[header] = 'bigint';
      return;
    }
    if (values.every(v => /^-?\d*\.?\d+([eE][+-]?\d+)?$/.test(v))) {
      const maxDec = Math.max(...values.map(v => { const p = v.split('.'); return p[1] ? p[1].length : 0; }));
      types[header] = maxDec <= 2 ? 'decimal_money' : 'decimal';
      return;
    }
    if (values.every(v => /^(true|false|yes|no|0|1|t|f|y|n)$/i.test(v))) { types[header] = 'bool'; return; }

    const dateCount = values.filter(v => isValidDate(v)).length;
    if (dateCount > values.length * 0.7) { types[header] = 'date'; return; }

    const emailCount = values.filter(v => looksLikeEmail(v)).length;
    if (emailCount > values.length * 0.7) { types[header] = 'email'; return; }

    const maxLen = Math.max(...values.map(v => v.length));
    if (maxLen > 5000) types[header] = 'longtext';
    else if (maxLen > 500) types[header] = 'text';
    else if (maxLen > 255) types[header] = 'varchar_long';
    else types[header] = 'varchar';
  });
  return types;
}

// ── SQL GENERATION ────────────────────────────────────────────────────────

export function generateCreateTable(tableName: string, headers: string[], types: ColumnTypes, pkColumn: string): string {
  const typeMap: Record<string, string> = {
    tinyint: 'TINYINT', smallint: 'SMALLINT', int: 'INT', bigint: 'BIGINT',
    decimal: 'DECIMAL(15,4)', decimal_money: 'DECIMAL(10,2)',
    bool: 'TINYINT(1)', date: 'DATE', text: 'TEXT', longtext: 'LONGTEXT',
    varchar: 'VARCHAR(255)', varchar_long: 'VARCHAR(500)', email: 'VARCHAR(320)',
  };
  let sql = `-- Generated by DataScrub Pro\n-- Date: ${new Date().toISOString()}\n\n`;
  sql += `CREATE DATABASE IF NOT EXISTS mydb\n  CHARACTER SET utf8mb4\n  COLLATE utf8mb4_unicode_ci;\nUSE mydb;\n\n`;
  sql += `DROP TABLE IF EXISTS \`${tableName}\`;\n\n`;
  sql += `CREATE TABLE \`${tableName}\` (\n`;
  const defs = headers.map(h => {
    const sqlType = typeMap[types[h]] || 'TEXT';
    const isPK = h === pkColumn;
    const pk = isPK ? ' NOT NULL AUTO_INCREMENT PRIMARY KEY' : '';
    const pad = ' '.repeat(Math.max(1, 30 - h.length));
    return `    \`${h}\`${pad}${sqlType}${pk}`;
  });
  if (pkColumn && !headers.includes(pkColumn)) {
    const pad = ' '.repeat(Math.max(1, 30 - pkColumn.length));
    defs.unshift(`    \`${pkColumn}\`${pad}BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY`);
  }
  sql += defs.join(',\n');
  sql += '\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;';
  return sql;
}

export function generateLoadData(tableName: string, cleanedFileName: string, headers: string[]): string {
  let sql = `-- Import cleaned CSV data\n\n`;
  sql += `LOAD DATA LOCAL INFILE '${cleanedFileName}'\n`;
  sql += `INTO TABLE \`${tableName}\`\n`;
  sql += `CHARACTER SET utf8mb4\n`;
  sql += `FIELDS TERMINATED BY ','\n`;
  sql += `OPTIONALLY ENCLOSED BY '"'\n`;
  sql += `LINES TERMINATED BY '\\n'\n`;
  sql += `IGNORE 1 ROWS\n`;
  sql += `(${headers.map(h => '`' + h + '`').join(', ')});\n\n`;
  sql += `-- Verify\nSELECT COUNT(*) AS total_rows FROM \`${tableName}\`;\nSELECT * FROM \`${tableName}\` LIMIT 10;`;
  return sql;
}

// ── EOL ──

export function getEolChar(eol: string): string {
  switch (eol) {
    case 'CRLF': return '\r\n';
    case 'CR': return '\r';
    case 'LFCR': return '\n\r';
    case 'NEL': return '\u0085';
    case 'LS': return '\u2028';
    case 'PS': return '\u2029';
    default: return '\n';
  }
}

// ── UTILITIES ─────────────────────────────────────────────────────────────

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  if (bytes >= 1073741824) return (bytes / 1073741824).toFixed(2) + ' GB';
  if (bytes >= 1048576) return (bytes / 1048576).toFixed(2) + ' MB';
  if (bytes >= 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return bytes + ' B';
}

export function fuzzyNormalize(str: string): string {
  return str.toLowerCase().replace(/\s+/g, '').replace(/[^a-z0-9]/g, '');
}

// ── COLLECT ALL ROWS ──────────────────────────────────────────────────────

export function getAllDataRows(chunks: string[], maxRows?: number, separator: string = ','): string[][] {
  const rows: string[][] = [];
  for (let c = 0; c < chunks.length; c++) {
    const lines = chunks[c].split(/\r?\n|\r/);
    const startLine = c === 0 ? 1 : 0;
    for (let i = startLine; i < lines.length; i++) {
      const line = lines[i];
      if (!line.trim()) continue;
      rows.push(parseCSVLine(line, separator));
      if (maxRows && rows.length >= maxRows) return rows;
    }
  }
  return rows;
}

// ── MAIN PROCESSOR ────────────────────────────────────────────────────────

export interface ProcessResult {
  cleanedChunks: string[];
  headers: string[];
  columnTypes: ColumnTypes;
  stats: CleaningStats;
  separator: string;
}

export async function processFile(
  file: File,
  config: CleaningConfig,
  onProgress: (percent: number, label: string) => void,
  onLog: (entry: LogEntry) => void
): Promise<ProcessResult> {
  const CHUNK_SIZE = 1024 * 1024;
  let offset = 0;
  let leftover = '';
  const cleanedDataChunks: string[] = [];
  let rowBuffer: string[] = [];
  let separator = '';
  let isFirstChunk = true;

  // ── FIX: Two separate header arrays ──────────────────────────────────────
  // rawHeaders  = the columns as they appear in the SOURCE file (never has 'id' prepended)
  // fileHeaders = the final output columns (may have 'id' prepended or an existing id replaced)
  let rawHeaders: string[] = [];
  let fileHeaders: string[] = [];

  // When generateId is true, this tracks whether the source already had an 'id' column
  // and at which index, so we can skip it while mapping source cells → output cells.
  let sourceIdColIndex = -1;   // index of 'id' in rawHeaders (-1 = not present)
  let hasGeneratedId = false;  // true when we are injecting a sequential id column

  const seenHashes = new Set<string>();
  const fuzzyHashes = new Set<string>();
  let totalOriginalRows = 0;
  let totalCleanedRows = 0;
  let totalFixedCells = 0;
  let fuzzyDupCount = 0;
  let columnMismatchCount = 0;
  let idCounter = 1;

  const eolChar = getEolChar(config.eol);
  const numericColumns: Record<number, number[]> = {};

  const addLog = (icon: string, message: string, type: '' | 'success' | 'warn' | 'error' = '') => {
    onLog({ icon, message, type });
  };

  addLog('📂', `Mode: ${config.mode.toUpperCase()} | File: ${file.name} (${formatBytes(file.size)})`);
  addLog('⚙️', `EOL: ${config.eol} | Encoding: ${config.encoding}`);

  while (offset < file.size) {
    const chunk = file.slice(offset, offset + CHUNK_SIZE);
    const text = await readChunkAsText(chunk);
    offset += CHUNK_SIZE;

    const readProgress = Math.min(50, (offset / file.size) * 50);
    onProgress(readProgress, `Reading... ${formatBytes(Math.min(offset, file.size))} / ${formatBytes(file.size)}`);

    const fullText = leftover + text;
    const lines = fullText.split(/\r?\n|\r/);
    leftover = offset < file.size ? (lines.pop() || '') : '';

    // ── HEADER PROCESSING (first line of first chunk only) ─────────────────
    if (isFirstChunk && lines.length > 0) {
      if (lines[0].charCodeAt(0) === 0xFEFF) lines[0] = lines[0].substring(1);

      separator = detectSeparator(lines[0]);
      const sepName = separator === ',' ? 'Comma' : separator === '\t' ? 'Tab' : separator === ';' ? 'Semicolon' : separator === '|' ? 'Pipe' : 'Unknown';
      addLog('🔍', `Separator: ${sepName} detected`);

      // Parse raw headers from the source file
      rawHeaders = parseCSVLine(lines[0], separator).map(h => sanitizeColumnName(h));

      // De-duplicate column names
      const nameCount: Record<string, number> = {};
      rawHeaders = rawHeaders.map(h => {
        if (nameCount[h] !== undefined) { nameCount[h]++; return `${h}_${nameCount[h]}`; }
        nameCount[h] = 0; return h;
      });

      addLog('📊', `Found ${rawHeaders.length} columns: ${rawHeaders.slice(0, 5).join(', ')}${rawHeaders.length > 5 ? '...' : ''}`);

      // ── FIX: Build fileHeaders and track source id column cleanly ─────────
      if (config.generateId) {
        sourceIdColIndex = rawHeaders.findIndex(h => h.toLowerCase() === 'id');

        if (sourceIdColIndex === -1) {
          // No id in source → prepend 'id' to output headers, keep all raw columns
          fileHeaders = ['id', ...rawHeaders];
          hasGeneratedId = true;
          addLog('🆔', `No ID column found — adding new "id" column as first column`);
        } else {
          // Source already has 'id' → move it to front in output, regenerate values
          // Remove it from its current position, then put it at index 0
          const withoutId = rawHeaders.filter((_, i) => i !== sourceIdColIndex);
          fileHeaders = ['id', ...withoutId];
          hasGeneratedId = true;
          addLog('🆔', `Existing "id" column found at source index ${sourceIdColIndex} — moving to first position and regenerating IDs`);
        }
      } else {
        // No id generation — output headers == raw headers exactly
        fileHeaders = [...rawHeaders];
        hasGeneratedId = false;
        sourceIdColIndex = -1;
      }

      // Write header row — always done AFTER fileHeaders is finalised
      cleanedDataChunks.push(fileHeaders.map(h => quoteCell(h)).join(',') + eolChar);
      lines.shift();
      isFirstChunk = false;
    }

    // ── ROW PROCESSING ─────────────────────────────────────────────────────
    for (let li = 0; li < lines.length; li++) {
      let line = lines[li];
      if (!line.trim()) continue;
      totalOriginalRows++;

      try {
        // Merge lines with unbalanced quotes (multi-line fields)
        const countQuotes = (s: string) => (s.match(/"/g) || []).length;
        if (countQuotes(line) % 2 === 1) {
          let merges = 0;
          while (li + 1 < lines.length && countQuotes(line) % 2 === 1 && merges < 50) {
            li++;
            merges++;
            line += '\n' + lines[li];
          }
        }

        // Parse the row — result always maps to rawHeaders
        let rawCells = parseCSVLine(line, separator);

        // Normalise rawCells length to match rawHeaders (never fileHeaders)
        if (rawCells.length !== rawHeaders.length) {
          let merged = false;
          let attempt = line;
          for (let m = 1; m <= 10 && li + m < lines.length; m++) {
            attempt += '\n' + lines[li + m];
            const attemptCells = parseCSVLine(attempt, separator);
            if (attemptCells.length === rawHeaders.length) {
              rawCells = attemptCells;
              li += m;
              merged = true;
              break;
            }
          }

          if (!merged) {
            columnMismatchCount++;
            const originalCount = rawCells.length;
            if (rawCells.length < rawHeaders.length) {
              while (rawCells.length < rawHeaders.length) rawCells.push('');
            } else {
              rawCells = rawCells.slice(0, rawHeaders.length);
            }
            if (columnMismatchCount <= 5) {
              addLog('⚠️', `Column mismatch at row ${totalOriginalRows}: expected ${rawHeaders.length}, got ${originalCount}, normalized`, 'warn');
            }
          }
        }

        // ── FIX: Build outputCells mapped to fileHeaders ──────────────────
        // rawCells always aligns with rawHeaders.
        // fileHeaders = ['id', ...rawHeaders_without_original_id]   (when hasGeneratedId)
        //             = [...rawHeaders]                              (otherwise)
        //
        // We apply cleaning transforms to rawCells first (indexed by rawHeaders),
        // then assemble outputCells for fileHeaders.

        let cellsFixed = 0;

        // Apply cleaning transforms on rawCells (aligned to rawHeaders)
        if (config.fixEncoding) {
          rawCells = rawCells.map(c => { const f = fixEncodingIssues(c); if (f !== c) cellsFixed++; return f; });
        }
        if (config.trimWhitespace) {
          rawCells = rawCells.map(c => { const o = c; const t = (c || '').replace(/\s+/g, ' ').trim(); if (o !== t) cellsFixed++; return t; });
        }
        if (config.normalizeValues) {
          rawCells = rawCells.map(c => {
            const val = (c || '').trim();
            if (/^(null|NULL|Null|N\/A|n\/a|NA|na|none|None|NONE|undefined|Undefined|nil|NIL|\?|#N\/A|#VALUE!|#REF!|#NAME\?|#DIV\/0!|-|—|\.{2,})$/.test(val)) {
              cellsFixed++; return '';
            }
            return val;
          });
        }

        if (config.mode === 'advanced') {
          for (let i = 0; i < rawCells.length; i++) {
            // ── FIX: Never apply data transforms to the source id column.
            // That slot is always replaced by a fresh sequential id in outputCells,
            // so cleaning its old value (or using it for outlier / normalizeCase
            // column-name lookups) would corrupt data and misname columns.
            if (i === sourceIdColIndex) continue;

            const colName = rawHeaders[i]; // rawHeaders always aligns 1-to-1 with rawCells
            if (config.removeHtmlTags && rawCells[i] && /<[^>]+>/.test(rawCells[i])) {
              const f = removeHtmlTagsFn(rawCells[i]); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.validateEmail && looksLikeEmail(rawCells[i])) {
              const f = fixEmail(rawCells[i]); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.standardizePhone && isPhoneColumnName(colName) && looksLikePhone(rawCells[i])) {
              const f = standardizePhoneFn(rawCells[i]); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.standardizeDate && isValidDate(rawCells[i])) {
              const f = standardizeDateFn(rawCells[i]); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.normalizeCase && rawCells[i]) {
              const f = normalizeCaseFn(rawCells[i], colName); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.removeSpecialChars && rawCells[i]) {
              const f = removeSpecialCharsFn(rawCells[i]); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.fixNumberFormats && rawCells[i]) {
              const f = fixNumberFormat(rawCells[i]); if (f !== rawCells[i]) { rawCells[i] = f; cellsFixed++; }
            }
            if (config.detectOutliers && rawCells[i]) {
              const num = parseFloat(rawCells[i]);
              if (!isNaN(num)) {
                // ── FIX: Key numericColumns by rawHeaders index so the outlier
                // report later can always resolve the name via rawHeaders[colIdx],
                // regardless of whether an id column shifted fileHeaders by 1.
                if (!numericColumns[i]) numericColumns[i] = [];
                numericColumns[i].push(num);
              }
            }
          }
        }

        totalFixedCells += cellsFixed;

        // Skip empty rows (entirely empty)
        if (config.removeEmpty && rawCells.every(c => !c.trim())) continue;

        // Skip rows with any empty values (if user enabled this feature)
        if (config.removeRowsWithEmptyValues && rawCells.some(c => !c.trim())) {
          continue;
        }

        // Duplicate detection — exclude source id column so dedup compares actual
        // data values, not old (soon-to-be-replaced) id numbers.
        const dedupeKey = rawCells.filter((_, i) => i !== sourceIdColIndex).join('\x00');
        if (config.removeDuplicates) {
          if (seenHashes.has(dedupeKey)) continue;
          seenHashes.add(dedupeKey);
        }
        if (config.fuzzyDuplicates) {
          const fuzzyKey = rawCells.filter((_, i) => i !== sourceIdColIndex).map(c => fuzzyNormalize(c)).join('\x00');
          if (fuzzyHashes.has(fuzzyKey)) {
            fuzzyDupCount++;
            if (fuzzyDupCount <= 5) addLog('🔍', `Fuzzy duplicate removed at row ${totalOriginalRows}`, 'warn');
            continue;
          }
          fuzzyHashes.add(fuzzyKey);
        }

        // ── FIX: Assemble outputCells for fileHeaders with zero ambiguity ──
        let outputCells: string[];

        if (hasGeneratedId) {
          // fileHeaders = ['id', col_a, col_b, ...]  (id-from-source removed from its old slot)
          // rawCells    = [col_a, col_b, ..., (maybe id at sourceIdColIndex)]
          //
          // Build the data columns (everything except the source id column)
          const dataCells: string[] = [];
          for (let i = 0; i < rawHeaders.length; i++) {
            if (i === sourceIdColIndex) continue; // skip the original id value
            dataCells.push(rawCells[i]);
          }
          // outputCells = sequential id + data columns
          outputCells = [String(idCounter), ...dataCells];
          idCounter++;
        } else {
          outputCells = rawCells;
        }

        // Final safety clamp — should never be needed but guards against edge cases
        if (outputCells.length < fileHeaders.length) {
          while (outputCells.length < fileHeaders.length) outputCells.push('');
        }
        if (outputCells.length > fileHeaders.length) {
          outputCells = outputCells.slice(0, fileHeaders.length);
        }

        rowBuffer.push(outputCells.map(c => quoteCell(c)).join(','));
        totalCleanedRows++;

        if (rowBuffer.length >= 1000) {
          cleanedDataChunks.push(rowBuffer.join(eolChar) + eolChar);
          rowBuffer = [];
          await new Promise(r => setTimeout(r, 0));
        }
      } catch {
        addLog('⚠️', `Skipped malformed row ${totalOriginalRows}`, 'warn');
      }
    }

    if (totalOriginalRows % 5000 === 0 && totalOriginalRows > 0) {
      onProgress(50 + ((offset / file.size) * 30), `${config.mode === 'advanced' ? 'AI Processing' : 'Processing'}... ${totalCleanedRows.toLocaleString()} rows`);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  if (rowBuffer.length > 0) {
    cleanedDataChunks.push(rowBuffer.join(eolChar));
  }

  if (fuzzyDupCount > 5) addLog('🔍', `... and ${fuzzyDupCount - 5} more fuzzy duplicates removed`, 'warn');

  const stats: CleaningStats = {
    original: totalOriginalRows, cleaned: totalCleanedRows,
    removed: totalOriginalRows - totalCleanedRows, cols: fileHeaders.length, fixed: totalFixedCells,
  };

  addLog('✓', `Processed: ${stats.original.toLocaleString()} → ${stats.cleaned.toLocaleString()} rows`);
  if (stats.removed > 0) addLog('🗑️', `Removed: ${stats.removed.toLocaleString()} rows`, 'warn');
  if (config.mode === 'advanced' && stats.fixed > 0) addLog('🤖', `AI Fixed: ${stats.fixed.toLocaleString()} cells`, 'success');
  if (columnMismatchCount > 0) addLog('⚠️', `Column mismatches: ${columnMismatchCount.toLocaleString()} rows had incorrect column counts`, 'warn');

  onProgress(85, 'Detecting column types...');
  const sampleData = getAllDataRows(cleanedDataChunks, 500, separator);
  const columnTypes = detectColumnTypes(fileHeaders, sampleData);
  addLog('✓', 'Column types detected');

  if (config.detectOutliers && Object.keys(numericColumns).length > 0) {
    let outlierCount = 0;
    for (const [colIdx, values] of Object.entries(numericColumns)) {
      if (values.length < 10) continue;
      const sorted = [...values].sort((a, b) => a - b);
      const q1 = sorted[Math.floor(sorted.length * 0.25)];
      const q3 = sorted[Math.floor(sorted.length * 0.75)];
      const iqr = q3 - q1;
      const outliers = values.filter(v => v < q1 - 1.5 * iqr || v > q3 + 1.5 * iqr).length;
      if (outliers > 0) {
        outlierCount += outliers;
        // ── FIX: numericColumns is keyed by rawHeaders index, so always look up
        // the column name via rawHeaders — fileHeaders may be offset by 1 when an
        // id column was prepended, which would name the wrong column.
        addLog('📊', `Column "${rawHeaders[parseInt(colIdx)]}" has ${outliers} potential outliers`, 'warn');
      }
    }
    if (outlierCount === 0) addLog('📊', 'No outliers detected', 'success');
  }

  onProgress(100, 'Complete!');
  addLog('🎉', `${config.mode === 'advanced' ? 'Advanced AI' : 'Standard'} cleaning complete!`, 'success');

  return { cleanedChunks: cleanedDataChunks, headers: fileHeaders, columnTypes, stats, separator };
}

function readChunkAsText(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => resolve(e.target?.result as string);
    reader.onerror = () => reject(new Error('Failed to read chunk'));
    reader.readAsText(blob, 'UTF-8');
  });
}