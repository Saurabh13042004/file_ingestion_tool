import { useState } from 'react'
import axios from 'axios'
import toast from 'react-hot-toast'

function App() {
  const [source, setSource] = useState('clickhouse') // or 'file'
  const [clickhouseConfig, setClickhouseConfig] = useState({
    host: '',
    port: 9440,
    database: '',
    user: '',
    jwt_token: ''
  })
  const [selectedTable, setSelectedTable] = useState('')
  const [tables, setTables] = useState([])
  const [columns, setColumns] = useState([])
  const [selectedColumns, setSelectedColumns] = useState([])
  const [file, setFile] = useState(null)
  const [delimiter, setDelimiter] = useState(',')
  const [loading, setLoading] = useState(false)
  const [previewData, setPreviewData] = useState(null)

  const handleClickHouseConnect = async () => {
    try {
      setLoading(true)
      const response = await axios.post('/api/connect-clickhouse', clickhouseConfig)
      toast.success('Connected to ClickHouse successfully')
      
      // Fetch tables
      const tablesResponse = await axios.get('/api/tables', {
        params: clickhouseConfig
      })
      setTables(tablesResponse.data.tables)
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to connect to ClickHouse')
    } finally {
      setLoading(false)
    }
  }

  const handleTableSelect = async (table) => {
    try {
      setLoading(true)
      setSelectedTable(table)
      const response = await axios.get(`/api/columns/${table}`, {
        params: clickhouseConfig
      })
      setColumns(response.data.columns)
      setSelectedColumns([])
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to fetch columns')
    } finally {
      setLoading(false)
    }
  }

  const handlePreview = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`/api/preview/${selectedTable}`, {
        params: { ...clickhouseConfig, limit: 100 }
      })
      setPreviewData(response.data)
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to fetch preview data')
    } finally {
      setLoading(false)
    }
  }

  const handleClickHouseToFile = async () => {
    try {
      setLoading(true)
      const response = await axios.post('/api/clickhouse-to-file', {
        config: clickhouseConfig,
        selection: {
          columns: selectedColumns,
          table_name: selectedTable
        },
        delimiter
      })
      toast.success(`Successfully exported ${response.data.record_count} records`)
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to export data')
    } finally {
      setLoading(false)
    }
  }

  const handleFileToClickHouse = async () => {
    if (!file) {
      toast.error('Please select a file first')
      return
    }

    try {
      setLoading(true)
      const formData = new FormData()
      formData.append('file', file)
      formData.append('table_name', selectedTable || 'imported_data')
      formData.append('config', JSON.stringify(clickhouseConfig))
      formData.append('delimiter', delimiter)

      const response = await axios.post('/api/file-to-clickhouse', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      })
      toast.success(`Successfully imported ${response.data.record_count} records`)
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to import data')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen p-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-3xl font-bold mb-8">ClickHouse & Flat File Data Ingestion</h1>
        
        <div className="card mb-6">
          <h2 className="text-xl font-semibold mb-4">Source Selection</h2>
          <div className="flex gap-4">
            <button
              className={`btn ${source === 'clickhouse' ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setSource('clickhouse')}
            >
              ClickHouse
            </button>
            <button
              className={`btn ${source === 'file' ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => setSource('file')}
            >
              Flat File
            </button>
          </div>
        </div>

        {source === 'clickhouse' && (
          <div className="card mb-6">
            <h2 className="text-xl font-semibold mb-4">ClickHouse Configuration</h2>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="label">Host</label>
                <input
                  type="text"
                  className="input"
                  value={clickhouseConfig.host}
                  onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, host: e.target.value })}
                />
              </div>
              <div>
                <label className="label">Port</label>
                <input
                  type="number"
                  className="input"
                  value={clickhouseConfig.port}
                  onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, port: parseInt(e.target.value) })}
                />
              </div>
              <div>
                <label className="label">Database</label>
                <input
                  type="text"
                  className="input"
                  value={clickhouseConfig.database}
                  onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, database: e.target.value })}
                />
              </div>
              <div>
                <label className="label">User</label>
                <input
                  type="text"
                  className="input"
                  value={clickhouseConfig.user}
                  onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, user: e.target.value })}
                />
              </div>
              <div className="col-span-2">
                <label className="label">JWT Token</label>
                <input
                  type="password"
                  className="input"
                  value={clickhouseConfig.jwt_token}
                  onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, jwt_token: e.target.value })}
                />
              </div>
            </div>
            <button
              className="btn btn-primary mt-4"
              onClick={handleClickHouseConnect}
              disabled={loading}
            >
              {loading ? 'Connecting...' : 'Connect'}
            </button>
          </div>
        )}

        {source === 'file' && (
          <div className="card mb-6">
            <h2 className="text-xl font-semibold mb-4">File Configuration</h2>
            <div className="space-y-4">
              <div>
                <label className="label">Select File</label>
                <input
                  type="file"
                  className="input"
                  onChange={(e) => setFile(e.target.files[0])}
                  accept=".csv,.txt"
                />
              </div>
              <div>
                <label className="label">Delimiter</label>
                <input
                  type="text"
                  className="input"
                  value={delimiter}
                  onChange={(e) => setDelimiter(e.target.value)}
                  maxLength={1}
                />
              </div>
            </div>
          </div>
        )}

        {tables.length > 0 && (
          <div className="card mb-6">
            <h2 className="text-xl font-semibold mb-4">Table Selection</h2>
            <select
              className="input"
              value={selectedTable}
              onChange={(e) => handleTableSelect(e.target.value)}
            >
              <option value="">Select a table</option>
              {tables.map((table) => (
                <option key={table} value={table}>
                  {table}
                </option>
              ))}
            </select>
          </div>
        )}

        {columns.length > 0 && (
          <div className="card mb-6">
            <h2 className="text-xl font-semibold mb-4">Column Selection</h2>
            <div className="grid grid-cols-3 gap-4">
              {columns.map((column) => (
                <label key={column} className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    checked={selectedColumns.includes(column)}
                    onChange={(e) => {
                      if (e.target.checked) {
                        setSelectedColumns([...selectedColumns, column])
                      } else {
                        setSelectedColumns(selectedColumns.filter((c) => c !== column))
                      }
                    }}
                    className="rounded border-gray-300"
                  />
                  <span>{column}</span>
                </label>
              ))}
            </div>
            <button
              className="btn btn-primary mt-4"
              onClick={handlePreview}
              disabled={loading || selectedColumns.length === 0}
            >
              Preview Data
            </button>
          </div>
        )}

        {previewData && (
          <div className="card mb-6">
            <h2 className="text-xl font-semibold mb-4">Data Preview</h2>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead>
                  <tr>
                    {previewData.columns.map((column) => (
                      <th
                        key={column}
                        className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                      >
                        {column}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {previewData.data.map((row, i) => (
                    <tr key={i}>
                      {row.map((cell, j) => (
                        <td key={j} className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {cell}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        <div className="card">
          <h2 className="text-xl font-semibold mb-4">Actions</h2>
          <div className="flex gap-4">
            {source === 'clickhouse' && (
              <button
                className="btn btn-primary"
                onClick={handleClickHouseToFile}
                disabled={loading || selectedColumns.length === 0}
              >
                {loading ? 'Exporting...' : 'Export to File'}
              </button>
            )}
            {source === 'file' && (
              <button
                className="btn btn-primary"
                onClick={handleFileToClickHouse}
                disabled={loading || !file}
              >
                {loading ? 'Importing...' : 'Import to ClickHouse'}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default App 