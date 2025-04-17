import { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Tabs,
  Tab,
  TextField,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  ListItemText,
  OutlinedInput,
  Alert,
  CircularProgress,
  Grid,
} from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

function DataIngestion() {
  // State for source selection
  const [activeTab, setActiveTab] = useState(0);
  const [sourceType, setSourceType] = useState('clickhouse');
  const [selectedTable, setSelectedTable] = useState('');

  // ClickHouse connection state
  const [chConfig, setChConfig] = useState({
    host: '',
    port: 9000,
    database: '',
    user: '',
    password: '',
    secure: false,
    verify: true,
  });

  // File upload state
  const [selectedFile, setSelectedFile] = useState(null);
  const [fileInfo, setFileInfo] = useState(null);

  // Data state
  const [tables, setTables] = useState([]);
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [previewData, setPreviewData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  // Handle tab change
  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
    setSourceType(newValue === 0 ? 'clickhouse' : 'flatfile');
    setSelectedTable('');
    setSelectedColumns([]);
    setPreviewData([]);
  };

  // Handle ClickHouse connection
  const handleConnect = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Clean up the host value by removing any protocol prefix and trailing slashes
      const cleanConfig = {
        ...chConfig,
        host: chConfig.host.replace(/^https?:\/\//, '').replace(/\/$/, '').split(':')[0] // Remove port if present in host
      };
      
      const response = await axios.post(`${API_BASE_URL}/clickhouse/connect`, cleanConfig);
      setTables(response.data.tables);
      setSuccess('Successfully connected to ClickHouse');
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to connect to ClickHouse');
    } finally {
      setLoading(false);
    }
  };

  // Handle file upload
  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    try {
      setLoading(true);
      setError(null);
      const response = await axios.post(`${API_BASE_URL}/flatfile/upload`, formData);
      setFileInfo(response.data.file_info);
      setSelectedFile(file);
      setSuccess('File uploaded successfully');
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to upload file');
    } finally {
      setLoading(false);
    }
  };

  // Handle column selection
  const handleColumnChange = (event) => {
    const { value } = event.target;
    setSelectedColumns(value);
  };

  // Handle data preview
  const handlePreview = async () => {
    try {
      setLoading(true);
      setError(null);
      let response;
      
      if (sourceType === 'clickhouse') {
        response = await axios.get(`${API_BASE_URL}/clickhouse/tables/${selectedTable}/columns`);
        setColumns(response.data);
      } else {
        response = await axios.get(`${API_BASE_URL}/flatfile/preview/${fileInfo.filename}`);
        setPreviewData(response.data.data);
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load preview');
    } finally {
      setLoading(false);
    }
  };

  // Handle data ingestion
  const handleIngestion = async () => {
    try {
      setLoading(true);
      setError(null);
      
      if (sourceType === 'clickhouse') {
        // Export from ClickHouse to Flat File
        const response = await axios.post(`${API_BASE_URL}/clickhouse/export`, {
          table_name: selectedTable,
          columns: selectedColumns,
          query: `SELECT ${selectedColumns.join(', ')} FROM ${selectedTable}`,
        });
        setSuccess(`Exported ${response.data.record_count} records successfully`);
      } else {
        // Import from Flat File to ClickHouse
        const response = await axios.post(`${API_BASE_URL}/clickhouse/import`, {
          table_name: selectedTable,
          file_path: fileInfo.file_path,
          columns: selectedColumns,
        });
        setSuccess(`Imported ${response.data.record_count} records successfully`);
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to process data');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box sx={{ width: '100%' }}>
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          Data Ingestion
        </Typography>
        
        <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
          <Tab label="ClickHouse" />
          <Tab label="Flat File" />
        </Tabs>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {success && (
          <Alert severity="success" sx={{ mb: 2 }}>
            {success}
          </Alert>
        )}

        {activeTab === 0 ? (
          // ClickHouse Configuration
          <Box>
            <Grid container spacing={2}>
              <Grid item xs={12} sm={6}>
                <TextField
                  fullWidth
                  label="Host"
                  value={chConfig.host}
                  onChange={(e) => setChConfig({ ...chConfig, host: e.target.value })}
                />
              </Grid>
              <Grid item xs={12} sm={6}>
                <TextField
                  fullWidth
                  label="Port"
                  type="number"
                  value={chConfig.port}
                  onChange={(e) => setChConfig({ ...chConfig, port: parseInt(e.target.value) })}
                />
              </Grid>
              <Grid item xs={12} sm={6}>
                <TextField
                  fullWidth
                  label="Database"
                  value={chConfig.database}
                  onChange={(e) => setChConfig({ ...chConfig, database: e.target.value })}
                />
              </Grid>
              <Grid item xs={12} sm={6}>
                <TextField
                  fullWidth
                  label="User"
                  value={chConfig.user}
                  onChange={(e) => setChConfig({ ...chConfig, user: e.target.value })}
                />
              </Grid>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Password"
                  type="password"
                  value={chConfig.password}
                  onChange={(e) => setChConfig({ ...chConfig, password: e.target.value })}
                />
              </Grid>
            </Grid>
            <Button
              variant="contained"
              onClick={handleConnect}
              disabled={loading}
              sx={{ mt: 2 }}
            >
              {loading ? <CircularProgress size={24} /> : 'Connect'}
            </Button>
          </Box>
        ) : (
          // Flat File Upload
          <Box>
            <Button
              variant="contained"
              component="label"
              disabled={loading}
            >
              Upload File
              <input
                type="file"
                hidden
                accept=".csv,.txt"
                onChange={handleFileUpload}
              />
            </Button>
            {fileInfo && (
              <Typography sx={{ mt: 2 }}>
                File: {fileInfo.filename} ({fileInfo.row_count} rows)
              </Typography>
            )}
          </Box>
        )}

        {tables.length > 0 && (
          <FormControl fullWidth sx={{ mt: 3 }}>
            <InputLabel>Select Table</InputLabel>
            <Select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
            >
              {tables.map((table) => (
                <MenuItem key={table.name} value={table.name}>
                  {table.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        )}

        {columns.length > 0 && (
          <FormControl fullWidth sx={{ mt: 3 }}>
            <InputLabel>Select Columns</InputLabel>
            <Select
              multiple
              value={selectedColumns}
              onChange={handleColumnChange}
              input={<OutlinedInput label="Select Columns" />}
              renderValue={(selected) => selected.join(', ')}
            >
              {columns.map((column) => (
                <MenuItem key={column.name} value={column.name}>
                  <Checkbox checked={selectedColumns.indexOf(column.name) > -1} />
                  <ListItemText primary={column.name} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        )}

        <Box sx={{ mt: 3 }}>
          <Button
            variant="contained"
            onClick={handlePreview}
            disabled={loading || (!selectedTable && !fileInfo)}
            sx={{ mr: 2 }}
          >
            Preview
          </Button>
          <Button
            variant="contained"
            color="primary"
            onClick={handleIngestion}
            disabled={loading || selectedColumns.length === 0}
          >
            {loading ? <CircularProgress size={24} /> : 'Start Ingestion'}
          </Button>
        </Box>

        {previewData.length > 0 && (
          <Box sx={{ mt: 3, height: 400 }}>
            <DataGrid
              rows={previewData}
              columns={selectedColumns.map((col) => ({ field: col, headerName: col }))}
              pageSize={5}
              rowsPerPageOptions={[5]}
              disableSelectionOnClick
            />
          </Box>
        )}
      </Paper>
    </Box>
  );
}

export default DataIngestion; 