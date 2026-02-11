import { useState, useRef, useCallback } from 'react';
import {
  type CleaningConfig,
  type CleaningStats,
  type LogEntry,
  type ColumnTypes,
  processFile,
  formatBytes,
  generateCreateTable,
  generateLoadData,
  getAllDataRows,
  storeResult,
  downloadCSV,
  downloadSQL,
  downloadJSON,
} from './utils/cleaningEngine';

type CleaningMode = 'standard' | 'advanced';

export function App() {
  const [file, setFile] = useState<File | null>(null);
  const [fileName, setFileName] = useState('');
  const [fileSize, setFileSize] = useState(0);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [mode, setMode] = useState<CleaningMode>('standard');

  const [tableName, setTableName] = useState('my_data');
  const [pkColumn, setPkColumn] = useState('');
  const [eolFormat, setEolFormat] = useState('Auto');
  const [encoding, setEncoding] = useState('UTF-8');

  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null);
  const [hasIdColumn, setHasIdColumn] = useState(false);
  const [generateId, setGenerateId] = useState(false);

  const [removeDuplicates, setRemoveDuplicates] = useState(true);
  const [removeEmpty, setRemoveEmpty] = useState(true);
  const [trimWhitespace, setTrimWhitespace] = useState(true);
  const [normalizeValues, setNormalizeValues] = useState(true);
  const [fixEncoding, setFixEncoding] = useState(true);

  const [fuzzyDuplicates, setFuzzyDuplicates] = useState(true);
  const [validateEmail, setValidateEmail] = useState(true);
  const [standardizePhone, setStandardizePhone] = useState(true);
  const [normalizeCase, setNormalizeCase] = useState(true);
  const [standardizeDate, setStandardizeDate] = useState(true);
  const [detectOutliers, setDetectOutliers] = useState(true);
  const [removeSpecialChars, setRemoveSpecialChars] = useState(true);
  const [crossFieldValidation, setCrossFieldValidation] = useState(true);
  const [fillMissing, setFillMissing] = useState(false);
  const [standardizeAddress, setStandardizeAddress] = useState(false);
  const [removeHtmlTags, setRemoveHtmlTags] = useState(true);
  const [fixNumberFormats, setFixNumberFormats] = useState(true);
  const [removeRowsWithEmptyValues, setRemoveRowsWithEmptyValues] = useState(false);

  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [progressLabel, setProgressLabel] = useState('');
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [showProgress, setShowProgress] = useState(false);

  const [showResults, setShowResults] = useState(false);
  const [stats, setStats] = useState<CleaningStats>({ original: 0, cleaned: 0, removed: 0, cols: 0, fixed: 0 });
  const [resultHeaders, setResultHeaders] = useState<string[]>([]);
  const [resultColumnTypes, setResultColumnTypes] = useState<ColumnTypes>({});
  const [previewRows, setPreviewRows] = useState<string[][]>([]);
  const [createSQLDisplay, setCreateSQLDisplay] = useState('');
  const [loadSQLDisplay, setLoadSQLDisplay] = useState('');
  const [outputSize, setOutputSize] = useState(0);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const [showColumnSelector, setShowColumnSelector] = useState(false);
  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set());
  const [appliedColumns, setAppliedColumns] = useState<Set<string>>(new Set());

  const logBoxRef = useRef<HTMLDivElement>(null);

  const showToast = useCallback((message: string, type: 'success' | 'error' = 'success') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  }, []);

  const handleFile = useCallback((f: File) => {
    if (!f.name.match(/\.(csv|tsv|txt)$/i)) {
      alert('Please upload a CSV, TSV, or TXT file.');
      return;
    }
    setFile(f);
    setFileName(f.name);
    setFileSize(f.size);
    const baseName = f.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
    setTableName(baseName);
    setShowResults(false);
    setShowProgress(false);
    setLogs([]);
    
    // Check if file has id column
    const reader = new FileReader();
    reader.onload = (e) => {
      const text = e.target?.result as string;
      const firstLine = text.split(/\r?\n|\r/)[0];
      
      // Detect separator
      const counts = {
        ',': (firstLine.match(/,/g) || []).length,
        '\t': (firstLine.match(/\t/g) || []).length,
        ';': (firstLine.match(/;/g) || []).length,
        '|': (firstLine.match(/\|/g) || []).length,
      };
      const separator = Object.entries(counts).sort((a, b) => b[1] - a[1])[0][0];
      
      // Split by detected separator and check for id column
      const headers = firstLine.split(separator).map(h => h.trim().toLowerCase().replace(/"/g, ''));
      setHasIdColumn(headers.includes('id'));
      // Allow ID generation even if column doesn't exist
      setGenerateId(false); // Default to false - user can toggle anytime
    };
    reader.readAsText(f.slice(0, 1024)); // Read only first 1KB to check headers
  }, []);

  const removeFile = useCallback(() => {
    setFile(null);
    setFileName('');
    setFileSize(0);
    setShowResults(false);
    setShowProgress(false);
    setLogs([]);
    setSelectedColumns(new Set());
    setAppliedColumns(new Set());
    if (fileInputRef.current) fileInputRef.current.value = '';
  }, []);

  const startCleaning = useCallback(async () => {
    if (!file) return;
    setIsProcessing(true);
    setShowProgress(true);
    setShowResults(false);
    setLogs([]);
    setProgress(0);
    setProgressLabel('Initializing...');
    setSelectedColumns(new Set());
    setAppliedColumns(new Set());

    const config: CleaningConfig = {
      tableName: tableName || 'my_data', pkColumn, eol: eolFormat, encoding, mode, generateId,
      removeDuplicates, removeEmpty, trimWhitespace, normalizeValues, fixEncoding,
      fuzzyDuplicates: mode === 'advanced' && fuzzyDuplicates,
      validateEmail: mode === 'advanced' && validateEmail,
      standardizePhone: mode === 'advanced' && standardizePhone,
      normalizeCase: mode === 'advanced' && normalizeCase,
      standardizeDate: mode === 'advanced' && standardizeDate,
      detectOutliers: mode === 'advanced' && detectOutliers,
      removeSpecialChars: mode === 'advanced' && removeSpecialChars,
      crossFieldValidation: mode === 'advanced' && crossFieldValidation,
      fillMissing: mode === 'advanced' && fillMissing,
      standardizeAddress: mode === 'advanced' && standardizeAddress,
      removeHtmlTags: mode === 'advanced' && removeHtmlTags,
      fixNumberFormats: mode === 'advanced' && fixNumberFormats,
      removeRowsWithEmptyValues: mode === 'advanced' && removeRowsWithEmptyValues,
    };

    try {
      const result = await processFile(file, config,
        (pct, label) => { setProgress(pct); setProgressLabel(label); },
        (entry) => {
          setLogs(prev => [...prev, entry]);
          setTimeout(() => { if (logBoxRef.current) logBoxRef.current.scrollTop = logBoxRef.current.scrollHeight; }, 10);
        }
      );

      const cleanedFileName = file.name.replace(/\.[^.]+$/, '') + '_cleaned.csv';
      const cSQL = generateCreateTable(config.tableName, result.headers, result.columnTypes, config.pkColumn);
      const lSQL = generateLoadData(config.tableName, cleanedFileName, result.headers);

      // Store for downloads
      storeResult({
        chunks: result.cleanedChunks, headers: result.headers,
        fileName: file.name, createSQL: cSQL, loadSQL: lSQL,
        separator: result.separator,
      });

      setStats(result.stats);
      setResultHeaders(result.headers);
      setResultColumnTypes(result.columnTypes);
      setPreviewRows(getAllDataRows(result.cleanedChunks, 15, result.separator));
      setCreateSQLDisplay(cSQL);
      setLoadSQLDisplay(lSQL);
      setOutputSize(result.cleanedChunks.reduce((s, c) => s + new Blob([c]).size, 0));

      await new Promise(r => setTimeout(r, 300));
      setShowResults(true);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      setLogs(prev => [...prev, { icon: '❌', message: `Error: ${msg}`, type: 'error' }]);
    } finally {
      setIsProcessing(false);
    }
  }, [file, tableName, pkColumn, eolFormat, encoding, mode, generateId, removeDuplicates, removeEmpty, trimWhitespace, normalizeValues, fixEncoding, fuzzyDuplicates, validateEmail, standardizePhone, normalizeCase, standardizeDate, detectOutliers, removeSpecialChars, crossFieldValidation, fillMissing, standardizeAddress, removeHtmlTags, fixNumberFormats, removeRowsWithEmptyValues]);

  const copyToClipboard = useCallback(async (text: string, id: string) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.left = '-9999px';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  }, []);

  const toggleColumnSelection = (colName: string) => {
    const newSelected = new Set(selectedColumns);
    if (newSelected.has(colName)) {
      newSelected.delete(colName);
    } else {
      newSelected.add(colName);
    }
    setSelectedColumns(newSelected);
  };

  const selectAllColumns = () => {
    setSelectedColumns(new Set(resultHeaders));
  };

  const deselectAllColumns = () => {
    setSelectedColumns(new Set());
  };

  const applyColumnSelection = () => {
    setAppliedColumns(new Set(selectedColumns));
    setShowColumnSelector(false);
    showToast(`Applied! Downloading ${selectedColumns.size} column${selectedColumns.size === 1 ? '' : 's'}`, 'success');
  };

  const hasFile = !!file;

  return (
    <div className="app-root">
      <div className="grid-bg" />
      <div className="orb orb-1" />
      <div className="orb orb-2" />

      {/* TOAST NOTIFICATION */}
      {toast && (
        <div className={`toast toast-${toast.type}`}>
          {toast.type === 'success' ? '✓' : '⚠'} {toast.message}
        </div>
      )}

      <div className="container">
        {/* HEADER */}
        <header className="header">
          <div className="logo-row">
            <div className="logo-icon">🧹</div>
            <div className="logo-text">DataScrub<span>Pro</span></div>
          </div>
          <div className="tagline">// Professional Dataset Cleaner with AI-Powered Advanced Mode</div>
        </header>

        {/* HERO */}
        <section className="hero">
          <h1>Clean datasets with <em>precision</em>.<br/>Standard or Advanced mode.</h1>
          <p>Choose Standard mode for fast, reliable cleaning. Or enable Advanced mode for AI-powered fuzzy matching, format standardization, and smart validation.</p>
          <div className="badges">
            <span className="badge"><span className="badge-dot" />Stream Processing</span>
            <span className="badge"><span className="badge-dot" />No Memory Crashes</span>
            <span className="badge badge-pro"><span className="badge-dot badge-dot-pro" />Advanced AI Mode</span>
            <span className="badge"><span className="badge-dot" />Smart Validation</span>
            <span className="badge"><span className="badge-dot" />MySQL Ready</span>
            <span className="badge"><span className="badge-dot" />JSON Export</span>
          </div>
        </section>

        {/* UPLOAD */}
        <section className="upload-section">
          <div
            className={`upload-zone ${isDragOver ? 'drag-over' : ''}`}
            onDragOver={(e) => { e.preventDefault(); setIsDragOver(true); }}
            onDragLeave={() => setIsDragOver(false)}
            onDrop={(e) => { e.preventDefault(); setIsDragOver(false); if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]); }}
            onClick={() => fileInputRef.current?.click()}
          >
            <span className="upload-icon">📁</span>
            <h2>Drop your CSV file here</h2>
            <p className="upload-sub-text">Stream-process files of any size with intelligent cleaning</p>
            <button className="btn-upload" onClick={(e) => { e.stopPropagation(); fileInputRef.current?.click(); }}>
              📂 Choose File
            </button>
            <p className="upload-hint">Supports CSV, TSV, TXT • Any file size</p>
          </div>
          <input ref={fileInputRef} type="file" accept=".csv,.tsv,.txt" style={{ display: 'none' }}
            onChange={(e) => { if (e.target.files?.length) handleFile(e.target.files[0]); }} />

          {hasFile && (
            <div className="file-info fade-in">
              <span className="file-icon-big">📄</span>
              <div className="file-details">
                <h3>{fileName}</h3>
                <p>{formatBytes(fileSize)} • {new Date().toLocaleDateString()}</p>
              </div>
              <button className="file-remove" onClick={removeFile}>Remove</button>
            </div>
          )}
        </section>

        {/* MODE SELECTOR */}
        {hasFile && (
          <div className="panel fade-in">
            <h3 className="panel-title">⚙️ Select Cleaning Mode</h3>
            <div className="mode-grid">
              <div className={`mode-tab ${mode === 'standard' ? 'active' : ''}`} onClick={() => setMode('standard')}>
                {mode === 'standard' && <span className="mode-check">✓</span>}
                <h4><span>🚀</span> Standard Cleaning <span className="badge badge-sm">Fast</span></h4>
                <p>Perfect for most datasets. Handles common cleaning tasks quickly.</p>
                <ul>
                  <li>Remove duplicates & empty rows</li>
                  <li>Trim whitespace & normalize nulls</li>
                  <li>Fix encoding & line endings</li>
                  <li>Auto-detect data types</li>
                </ul>
              </div>
              <div className={`mode-tab ${mode === 'advanced' ? 'active' : ''}`} onClick={() => setMode('advanced')}>
                {mode === 'advanced' && <span className="mode-check">✓</span>}
                <h4><span>🤖</span> Advanced Cleaning <span className="badge badge-sm badge-pro">AI-Powered</span></h4>
                <p>AI-powered deep cleaning with smart validation and format standardization.</p>
                <ul>
                  <li>Everything in Standard mode +</li>
                  <li>Fuzzy duplicate detection</li>
                  <li>Email & phone validation</li>
                  <li>Format standardization</li>
                  <li>HTML tag removal & more</li>
                </ul>
              </div>
            </div>
          </div>
        )}

        {/* CONFIG */}
        {hasFile && (
          <div className="panel fade-in">
            <h3 className="panel-title">⚙️ Configuration</h3>
            <div className="config-grid">
              <div className="config-item">
                <label>Table Name</label>
                <input type="text" value={tableName} onChange={(e) => setTableName(e.target.value)} placeholder="my_table" />
              </div>
              <div className="config-item">
                <label>Primary Key Column</label>
                <input type="text" value={pkColumn} onChange={(e) => setPkColumn(e.target.value)} placeholder="id (leave empty for none)" />
              </div>
              <div className="config-item">
                <label>EOL Format</label>
                <select value={eolFormat} onChange={(e) => setEolFormat(e.target.value)}>
                  <option value="Auto">Auto Detect</option>
                  <option value="LF">LF (Unix/Linux/macOS)</option>
                  <option value="CRLF">CRLF (Windows)</option>
                  <option value="CR">CR (Classic Mac)</option>
                </select>
              </div>
              <div className="config-item">
                <label>Encoding</label>
                <select value={encoding} onChange={(e) => setEncoding(e.target.value)}>
                  <option value="UTF-8">UTF-8</option>
                  <option value="UTF-8-BOM">UTF-8 with BOM</option>
                  <option value="UTF-16">UTF-16</option>
                  <option value="UTF-16LE">UTF-16 LE</option>
                  <option value="UTF-16BE">UTF-16 BE</option>
                  <option value="ASCII">ASCII</option>
                  <option value="ISO-8859-1">ISO-8859-1 (Latin-1)</option>
                  <option value="ISO-8859-15">ISO-8859-15 (Latin-9)</option>
                  <option value="Windows-1252">Windows-1252</option>
                  <option value="Windows-1251">Windows-1251 (Cyrillic)</option>
                  <option value="Shift_JIS">Shift_JIS (Japanese)</option>
                  <option value="EUC-JP">EUC-JP (Japanese)</option>
                  <option value="GB2312">GB2312 (Chinese Simplified)</option>
                  <option value="Big5">Big5 (Chinese Traditional)</option>
                  <option value="EUC-KR">EUC-KR (Korean)</option>
                  <option value="KOI8-R">KOI8-R (Russian)</option>
                </select>
              </div>
            </div>

            <div className="section-title">Standard Cleaning Options</div>
            <div className="toggle-row">
              <Toggle label="Remove Exact Duplicates" checked={removeDuplicates} onChange={setRemoveDuplicates} />
              <Toggle label="Remove Empty Rows" checked={removeEmpty} onChange={setRemoveEmpty} />
              <Toggle label="Trim Whitespace" checked={trimWhitespace} onChange={setTrimWhitespace} />
              <Toggle label="Normalize Null Values" checked={normalizeValues} onChange={setNormalizeValues} />
              <Toggle label="Fix Encoding Issues" checked={fixEncoding} onChange={setFixEncoding} />
            </div>
            <div className="toggle-row" style={{ marginTop: '12px' }}>
              <Toggle label="Generate/Add ID Column (1, 2, 3...)" checked={generateId} onChange={setGenerateId} />
            </div>

            {mode === 'advanced' && (
              <div className="advanced-box fade-in">
                <div className="advanced-header">
                  <span>🤖</span>
                  <h4>Advanced AI-Powered Features</h4>
                </div>
                <div className="toggle-row">
                  <Toggle label="Fuzzy Duplicate Detection" checked={fuzzyDuplicates} onChange={setFuzzyDuplicates} pro />
                  <Toggle label="Validate & Fix Emails" checked={validateEmail} onChange={setValidateEmail} pro />
                  <Toggle label="Standardize Phones" checked={standardizePhone} onChange={setStandardizePhone} pro />
                  <Toggle label="Smart Case Normalization" checked={normalizeCase} onChange={setNormalizeCase} pro />
                  <Toggle label="Standardize Dates" checked={standardizeDate} onChange={setStandardizeDate} pro />
                  <Toggle label="Detect Outliers" checked={detectOutliers} onChange={setDetectOutliers} pro />
                  <Toggle label="Remove Control Chars" checked={removeSpecialChars} onChange={setRemoveSpecialChars} pro />
                  <Toggle label="Cross-Field Validation" checked={crossFieldValidation} onChange={setCrossFieldValidation} pro />
                  <Toggle label="Remove HTML Tags" checked={removeHtmlTags} onChange={setRemoveHtmlTags} pro />
                  <Toggle label="Fix Number Formats" checked={fixNumberFormats} onChange={setFixNumberFormats} pro />
                  <Toggle label="Fill Missing Values" checked={fillMissing} onChange={setFillMissing} pro />
                  <Toggle label="Standardize Addresses" checked={standardizeAddress} onChange={setStandardizeAddress} pro />
                  <Toggle label="Remove Rows with Empty Values" checked={removeRowsWithEmptyValues} onChange={setRemoveRowsWithEmptyValues} pro />
                </div>
              </div>
            )}
          </div>
        )}

        {/* CLEAN BUTTON */}
        {hasFile && (
          <button className="clean-btn fade-in" disabled={isProcessing} onClick={startCleaning}>
            <span>{mode === 'advanced' ? '🤖' : '🧹'}</span>
            <span>{isProcessing ? 'Processing...' : mode === 'advanced' ? 'Advanced AI Clean Dataset' : 'Clean Dataset (Standard)'}</span>
          </button>
        )}

        {/* PROGRESS */}
        {showProgress && (
          <div className="panel fade-in">
            <div className="progress-label">
              <span className="step-name">{progressLabel}</span>
              <span className="pct">{Math.round(progress)}%</span>
            </div>
            <div className="progress-bar">
              <div className="progress-fill" style={{ width: `${progress}%` }} />
            </div>
            <div ref={logBoxRef} className="log-box">
              {logs.map((log, i) => (
                <div key={i} className={`log-line ${log.type}`}>
                  <span className="log-tag">{log.icon}</span>
                  <span className="log-msg">{log.message}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* RESULTS */}
        {showResults && (
          <div className="results fade-in">
            <div className="stats-grid">
              <StatCard value={stats.cleaned.toLocaleString()} label="Total Rows" />
              <StatCard value={String(stats.cols)} label="Columns" />
              <StatCard value={stats.removed > 0 ? `-${stats.removed.toLocaleString()}` : '0'} label="Rows Removed" color={stats.removed > 0 ? '#ef4444' : undefined} />
              <StatCard value={stats.fixed.toLocaleString()} label="Cells Fixed" color={stats.fixed > 0 ? '#10b981' : undefined} />
              <StatCard value={formatBytes(outputSize)} label="Output Size" />
            </div>

            {/* DOWNLOAD BUTTONS — call module functions directly */}
            <div className="download-grid">
              <button className="dl-btn" onClick={() => {
                if (downloadCSV(Array.from(appliedColumns).length > 0 ? Array.from(appliedColumns) : undefined)) {
                  showToast('CSV downloaded successfully! ✓');
                } else {
                  showToast('Failed to download CSV', 'error');
                }
              }}>
                <span className="dl-icon">⬇️</span> Download CSV
              </button>
              <button className="dl-btn" onClick={() => {
                if (downloadSQL(Array.from(appliedColumns).length > 0 ? Array.from(appliedColumns) : undefined)) {
                  showToast('SQL downloaded successfully! ✓');
                } else {
                  showToast('Failed to download SQL', 'error');
                }
              }}>
                <span className="dl-icon">🗄️</span> Download SQL
              </button>
              <button className="dl-btn" onClick={async () => {
                try {
                  const success = await downloadJSON(Array.from(appliedColumns).length > 0 ? Array.from(appliedColumns) : undefined);
                  if (success) {
                    showToast('✓ JSON downloaded successfully!');
                  } else {
                    showToast('✕ Failed to download JSON', 'error');
                  }
                } catch (err) {
                  console.error('Download error:', err);
                  showToast('✕ Download error occurred', 'error');
                }
              }}>
                <span className="dl-icon">📋</span> Download JSON
              </button>
              <button className="dl-btn" onClick={() => setShowColumnSelector(!showColumnSelector)}
                style={{ backgroundColor: showColumnSelector ? '#6366f1' : undefined }}>
                <span className="dl-icon">✓</span> {showColumnSelector ? 'Hide' : 'Select'} Columns
              </button>
            </div>

            {/* COLUMN SELECTOR MODAL */}
            {showColumnSelector && (
              <div className="column-selector fade-in">
                <div className="selector-header">
                  <h4>📋 Select Columns to Download</h4>
                  <p className="selector-info">Choose which columns to include ({selectedColumns.size} of {resultHeaders.length} selected)</p>
                </div>
                <div className="selector-actions">
                  <button className="selector-btn" onClick={selectAllColumns}>Select All</button>
                  <button className="selector-btn" onClick={deselectAllColumns}>Deselect All</button>
                </div>
                <div className="selector-grid">
                  {resultHeaders.map((col) => (
                    <label key={col} className="selector-item">
                      <input
                        type="checkbox"
                        checked={selectedColumns.has(col)}
                        onChange={() => toggleColumnSelection(col)}
                      />
                      <span className="selector-label">{col}</span>
                    </label>
                  ))}
                </div>
                <div className="selector-footer">
                  <button className="selector-apply-btn" onClick={applyColumnSelection} 
                    disabled={selectedColumns.size === 0}>
                    ✓ Apply Selected Columns
                  </button>
                </div>
              </div>
            )}

            {/* PREVIEW */}
            <div className="preview-box">
              {(() => {
                // Determine which columns to display in preview
                // Show selectedColumns in real-time if modal is open, otherwise show appliedColumns
                const activeSelection = showColumnSelector ? selectedColumns : appliedColumns;
                const displayHeaders = activeSelection.size > 0 
                  ? Array.from(activeSelection)
                  : resultHeaders;
                
                // Get indices of columns to display
                const displayIndices = displayHeaders.map(h => resultHeaders.indexOf(h));

                return (
                  <>
                    <h3 className="preview-title">📊 Data Preview ({previewRows.length} rows{activeSelection.size > 0 ? ` • ${activeSelection.size} selected columns` : ''})</h3>
                    <div className="table-wrap">
                      <table>
                        <thead>
                          <tr>
                            {displayHeaders.map((h, i) => (
                              <th key={`${h}-${i}`}>{h} <span className="col-type">({resultColumnTypes[h] || '?'})</span></th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {previewRows.map((row, ri) => (
                            <tr key={ri}>
                              {displayIndices.map((ci) => {
                                const cell = row[ci] || '';
                                return <td key={ci} title={cell}>{cell.length > 40 ? cell.substring(0, 40) + '…' : cell}</td>;
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                );
              })()}
            </div>

            {/* SQL */}
            <div className="sql-box">
              <h3 className="sql-title">
                <span>📝 CREATE TABLE Statement</span>
                <button className={`copy-btn ${copiedId === 'create' ? 'copied' : ''}`} onClick={() => copyToClipboard(createSQLDisplay, 'create')}>
                  {copiedId === 'create' ? '✓ Copied!' : 'Copy'}
                </button>
              </h3>
              <pre>{createSQLDisplay}</pre>
            </div>
            <div className="sql-box">
              <h3 className="sql-title">
                <span>📥 LOAD DATA Statement</span>
                <button className={`copy-btn ${copiedId === 'load' ? 'copied' : ''}`} onClick={() => copyToClipboard(loadSQLDisplay, 'load')}>
                  {copiedId === 'load' ? '✓ Copied!' : 'Copy'}
                </button>
              </h3>
              <pre>{loadSQLDisplay}</pre>
            </div>
          </div>
        )}

        <div className="footer">
          <div className="footer-content">
            <div className="footer-section">
              <h4>DataScrub Pro</h4>
              <p>Professional dataset cleaning with AI-powered advanced features</p>
            </div>
            <div className="footer-section">
              <h5>Features</h5>
              <ul>
                <li>Stream Processing</li>
                <li>Advanced AI Mode</li>
                <li>MySQL Ready</li>
                <li>JSON Export</li>
              </ul>
            </div>
            <div className="footer-section">
              <h5>Supported Formats</h5>
              <ul>
                <li>CSV Files</li>
                <li>TSV Files</li>
                <li>TXT Files</li>
                <li>Any File Size</li>
              </ul>
            </div>
            <div className="footer-section">
              <h5>Export Options</h5>
              <ul>
                <li>Download CSV</li>
                <li>Download SQL</li>
                <li>Download JSON</li>
                <li>Select Columns</li>
              </ul>
            </div>
          </div>
          <div className="footer-bottom">
            <p>&copy; 2026 DataScrub Pro. Built for data professionals. All rights reserved.</p>
            <p>Stream processing • No Memory Crashes • Smart Validation</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function Toggle({ label, checked, onChange, pro }: {
  label: string; checked: boolean; onChange: (v: boolean) => void; pro?: boolean;
}) {
  return (
    <button type="button" className={`toggle ${checked ? 'active' : ''} ${pro ? 'pro' : ''}`} onClick={() => onChange(!checked)}>
      <span className={`toggle-check ${checked ? 'checked' : ''}`}>✓</span>
      <span className="toggle-label">{label}</span>
    </button>
  );
}

function StatCard({ value, label, color }: { value: string; label: string; color?: string }) {
  return (
    <div className="stat-card">
      <div className="stat-val" style={color ? { color } : undefined}>{value}</div>
      <div className="stat-key">{label}</div>
    </div>
  );
}
