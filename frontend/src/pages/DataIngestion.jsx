import { useState, useEffect } from 'react';
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
  List,
  ListItem,
  ListItemButton,
  ListItemText as MuiListItemText,
  LinearProgress,
  FormControlLabel,
  Switch,
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
    host: 'clickhouse',
    port: 8123,
    database: 'default',
    user: 'default',
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

  // Flat File state
  const [exportedFiles, setExportedFiles] = useState([]);
  const [selectedFileData, setSelectedFileData] = useState([]);

  // Flat File import state
  const [fileToImport, setFileToImport] = useState(null);
  const [delimiter, setDelimiter] = useState(',');
  const [importTableName, setImportTableName] = useState('');
  const [importColumns, setImportColumns] = useState([]);
  const [filePreview, setFilePreview] = useState(null);
  const [isIngesting, setIsIngesting] = useState(false);
  const [progress, setProgress] = useState(0);

  // Multi-table JOIN state
  const [selectedTables, setSelectedTables] = useState([]);
  const [joinType, setJoinType] = useState('INNER');
  const [joinKeys, setJoinKeys] = useState({});
  const [isJoinMode, setIsJoinMode] = useState(false);

  // Handle tab change
  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
    setSourceType(newValue === 0 ? 'clickhouse' : 'flatfile');
    setSelectedTable('');
    setSelectedColumns([]);
    setPreviewData([]);
    if (newValue === 1) {
      fetchExportedFiles();
    }
  };

  // Fetch exported files
  const fetchExportedFiles = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE_URL}/files`);
      setExportedFiles(response.data);
      if (response.data.length > 0) {
        fetchFileData(response.data[0].id);
      }
    } catch (err) {
      setError('Failed to fetch exported files');
      console.error('Error fetching files:', err);
    } finally {
      setLoading(false);
    }
  };

  // Fetch data for a specific file
  const fetchFileData = async (fileId) => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE_URL}/files/${fileId}`);
      setSelectedFileData(response.data.records);
    } catch (err) {
      setError('Failed to fetch file data');
      console.error('Error fetching file data:', err);
    } finally {
      setLoading(false);
    }
  };

  // Handle ClickHouse connection
  const handleConnect = async () => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const response = await axios.post(`${API_BASE_URL}/clickhouse/connect`, chConfig);
      setTables(response.data || []);
      setSuccess('Successfully connected to ClickHouse');
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to connect to ClickHouse');
    } finally {
      setLoading(false);
    }
  };

  // Handle table selection
  const handleTableSelect = async (table) => {
    try {
      setLoading(true);
      setError(null);
      
      if (isJoinMode) {
        // Add table to selected tables for JOIN
        if (!selectedTables.includes(table)) {
          setSelectedTables([...selectedTables, table]);
        }
      } else {
        // Single table mode
        setSelectedTable(table);
        const response = await axios.get(`${API_BASE_URL}/clickhouse/tables/${table}/columns`);
        setColumns(response.data || []);
        setSelectedColumns([]);
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to get columns');
    } finally {
      setLoading(false);
    }
  };

  // Handle column selection
  const handleColumnChange = (event) => {
    setSelectedColumns(event.target.value);
  };

  // Handle preview
  const handlePreview = async () => {
    if (!selectedTable || selectedColumns.length === 0) {
      setError('Please select a table and at least one column');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      
      const query = `SELECT ${selectedColumns.join(', ')} FROM ${selectedTable} LIMIT 100`;
      const response = await axios.post(`${API_BASE_URL}/clickhouse/export`, {
        table_name: selectedTable,
        columns: selectedColumns,
        query: query,
        limit: 100
      });
      
      setPreviewData(response.data.records || []);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to preview data');
    } finally {
      setLoading(false);
    }
  };

  // Handle ingestion
  const handleIngestion = async () => {
    if (!selectedTable || selectedColumns.length === 0) {
      setError('Please select a table and at least one column');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      
      const query = `SELECT ${selectedColumns.join(', ')} FROM ${selectedTable}`;
      const response = await axios.post(`${API_BASE_URL}/clickhouse/export`, {
        table_name: selectedTable,
        columns: selectedColumns,
        query: query
      });
      
      setSuccess(`Successfully exported ${response.data.record_count} records`);
      // Refresh the exported files list
      if (activeTab === 1) {
        fetchExportedFiles();
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to export data');
    } finally {
      setLoading(false);
    }
  };

  const handleFileImport = async (event) => {
    const file = event.target.files[0];
    if (file) {
      setFileToImport(file);
      try {
        const formData = new FormData();
        formData.append('file', file);
        const response = await axios.post(`${API_BASE_URL}/flatfile/preview`, formData);
        setFilePreview(response.data);
        setImportColumns(response.data.columns.map(col => col.name));
      } catch (error) {
        setError('Failed to preview file');
        console.error('Error previewing file:', error);
      }
    }
  };

  const handleImportToClickHouse = async () => {
    if (!fileToImport || !importTableName || importColumns.length === 0) {
      setError('Please select a file, specify table name, and select columns');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setIsIngesting(true);
      setProgress(0);
      
      const formData = new FormData();
      formData.append('file', fileToImport);
      formData.append('table_name', importTableName);
      formData.append('columns', JSON.stringify(importColumns));
      formData.append('delimiter', delimiter);

      const response = await axios.post(`${API_BASE_URL}/flatfile/import`, formData);
      setSuccess(`Successfully imported ${response.data.record_count} records`);
      setFileToImport(null);
      setImportTableName('');
      setImportColumns([]);
      setFilePreview(null);
      setIsIngesting(false);
    } catch (error) {
      setError('Failed to import file');
      console.error('Error importing file:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleJoinKeyChange = (tablePair, key) => {
    setJoinKeys({
      ...joinKeys,
      [tablePair]: key
    });
  };

  const handleExport = async () => {
    try {
      setLoading(true);
      setError(null);
      
      let response;
      if (isJoinMode) {
        // Handle JOIN export
        if (selectedTables.length < 2) {
          setError('Please select at least two tables for JOIN');
          return;
        }
        
        const joinConditions = {
          tables: selectedTables,
          type: joinType,
          keys: joinKeys
        };
        
        response = await axios.post(`${API_BASE_URL}/clickhouse/export`, {
          columns: selectedColumns,
          join_conditions: joinConditions
        });
      } else {
        // Handle single table export
        response = await axios.post(`${API_BASE_URL}/clickhouse/export`, {
          table_name: selectedTable,
          columns: selectedColumns
        });
      }
      
      setSuccess(`Successfully exported ${response.data.record_count} records`);
      setPreviewData(response.data.records || []);
      
      // After successful export, switch to Flat File tab and refresh the files list
      setActiveTab(1);
      fetchExportedFiles();
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to export data');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          Data Ingestion Tool
        </Typography>
        
        {/* Status Messages at the top */}
        {success && (
          <Alert severity="success" sx={{ mb: 2 }}>
            {success}
          </Alert>
        )}
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        
        <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 3 }}>
          <Tab label="ClickHouse" />
          <Tab label="Flat File" />
        </Tabs>

        {activeTab === 0 && (
          <>
            <Grid container spacing={3}>
              {/* Connection Settings */}
              <Grid item xs={12}>
                <Paper sx={{ p: 2 }}>
                  <Typography variant="h6" gutterBottom>
                    Connection Settings
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={6} md={3}>
                      <TextField
                        fullWidth
                        label="Host"
                        value={chConfig.host}
                        onChange={(e) => setChConfig({ ...chConfig, host: e.target.value })}
                      />
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                      <TextField
                        fullWidth
                        label="Port"
                        type="number"
                        value={chConfig.port}
                        onChange={(e) => setChConfig({ ...chConfig, port: parseInt(e.target.value) })}
                      />
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                      <TextField
                        fullWidth
                        label="Database"
                        value={chConfig.database}
                        onChange={(e) => setChConfig({ ...chConfig, database: e.target.value })}
                      />
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                      <TextField
                        fullWidth
                        label="User"
                        value={chConfig.user}
                        onChange={(e) => setChConfig({ ...chConfig, user: e.target.value })}
                      />
                    </Grid>
                    <Grid item xs={12}>
                      <Button
                        variant="contained"
                        onClick={handleConnect}
                        disabled={loading}
                        sx={{ minWidth: 120 }}
                      >
                        {loading ? <CircularProgress size={24} /> : 'Connect'}
                      </Button>
                    </Grid>
                  </Grid>
                </Paper>
              </Grid>

              {/* Tables List */}
              {tables.length > 0 && (
                <Grid item xs={12} md={4}>
                  <Paper sx={{ p: 2, height: '100%' }}>
                    <Typography variant="h6" gutterBottom>
                      Available Tables
                    </Typography>
                    <List sx={{ maxHeight: 400, overflow: 'auto' }}>
                      {tables.map((table) => (
                        <ListItem key={table.name} disablePadding>
                          <ListItemButton
                            selected={selectedTable === table.name}
                            onClick={() => handleTableSelect(table.name)}
                          >
                            <MuiListItemText
                              primary={table.name}
                              secondary={`Engine: ${table.engine}, Rows: ${table.row_count}`}
                            />
                          </ListItemButton>
                        </ListItem>
                      ))}
                    </List>
                  </Paper>
                </Grid>
              )}

              {/* Columns Selection and Preview */}
              {columns.length > 0 && (
                <Grid item xs={12} md={8}>
                  <Paper sx={{ p: 2, height: '100%' }}>
                    <Typography variant="h6" gutterBottom>
                      Select Columns
                    </Typography>
                    <FormControl fullWidth sx={{ mb: 2 }}>
                      <InputLabel>Columns</InputLabel>
                      <Select
                        multiple
                        value={selectedColumns}
                        onChange={handleColumnChange}
                        input={<OutlinedInput label="Columns" />}
                        renderValue={(selected) => selected.join(', ')}
                      >
                        {columns.map((column) => (
                          <MenuItem key={column.name} value={column.name}>
                            <Checkbox checked={selectedColumns.indexOf(column.name) > -1} />
                            <ListItemText primary={column.name} secondary={column.type} />
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>

                    {selectedColumns.length > 0 && (
                      <>
                        <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
                          <Button
                            variant="contained"
                            onClick={handlePreview}
                            disabled={loading}
                            sx={{ minWidth: 120 }}
                          >
                            {loading ? <CircularProgress size={24} /> : 'Preview'}
                          </Button>
                          <Button
                            variant="contained"
                            color="primary"
                            onClick={handleExport}
                            disabled={loading}
                            sx={{ minWidth: 120 }}
                          >
                            {loading ? <CircularProgress size={24} /> : 'Start Ingestion'}
                          </Button>
                        </Box>

                        {isIngesting && (
                          <Box sx={{ width: '100%', mt: 2 }}>
                            <LinearProgress variant="determinate" value={progress} />
                            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                              {progress}% complete
                            </Typography>
                          </Box>
                        )}
                      </>
                    )}
                  </Paper>
                </Grid>
              )}

              {/* Preview Data */}
              {previewData.length > 0 && (
                <Grid item xs={12}>
                  <Paper sx={{ p: 2 }}>
                    <Typography variant="h6" gutterBottom>
                      Preview Data
                    </Typography>
                    <DataGrid
                      rows={previewData}
                      columns={selectedColumns.map(col => ({ 
                        field: col, 
                        headerName: col, 
                        width: 150,
                        flex: 1 
                      }))}
                      pageSize={5}
                      rowsPerPageOptions={[5, 10, 20]}
                      autoHeight
                      disableSelectionOnClick
                    />
                  </Paper>
                </Grid>
              )}
            </Grid>
          </>
        )}

        {activeTab === 1 && (
          <Grid container spacing={3}>
            {/* Import Section */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 2 }}>
                <Typography variant="h6" gutterBottom>
                  Import Flat File
                </Typography>
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <input
                      type="file"
                      accept=".csv,.txt"
                      onChange={handleFileImport}
                      style={{ display: 'none' }}
                      id="file-import"
                    />
                    <label htmlFor="file-import">
                      <Button
                        variant="contained"
                        component="span"
                        disabled={loading}
                        sx={{ mb: 2 }}
                      >
                        Select File
                      </Button>
                    </label>
                    {fileToImport && (
                      <Typography sx={{ ml: 2 }}>
                        Selected: {fileToImport.name}
                      </Typography>
                    )}
                  </Grid>
                  <Grid item xs={12} sm={6}>
                    <TextField
                      fullWidth
                      label="Table Name"
                      value={importTableName}
                      onChange={(e) => setImportTableName(e.target.value)}
                    />
                  </Grid>
                  <Grid item xs={12} sm={6}>
                    <TextField
                      fullWidth
                      label="Delimiter"
                      value={delimiter}
                      onChange={(e) => setDelimiter(e.target.value)}
                    />
                  </Grid>
                  {filePreview && (
                    <Grid item xs={12}>
                      <FormControl fullWidth>
                        <InputLabel>Columns</InputLabel>
                        <Select
                          multiple
                          value={importColumns}
                          onChange={(e) => setImportColumns(e.target.value)}
                          input={<OutlinedInput label="Columns" />}
                          renderValue={(selected) => selected.join(', ')}
                        >
                          {filePreview.columns.map((column) => (
                            <MenuItem key={column.name} value={column.name}>
                              <Checkbox checked={importColumns.indexOf(column.name) > -1} />
                              <ListItemText primary={column.name} secondary={column.type} />
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </Grid>
                  )}
                  <Grid item xs={12}>
                    <Button
                      variant="contained"
                      color="primary"
                      onClick={handleImportToClickHouse}
                      disabled={loading || !fileToImport || !importTableName || importColumns.length === 0}
                      sx={{ minWidth: 120 }}
                    >
                      {loading ? <CircularProgress size={24} /> : 'Import to ClickHouse'}
                    </Button>
                  </Grid>
                </Grid>
              </Paper>
            </Grid>

            {/* Exported Files Section */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 2 }}>
                <Typography variant="h6" gutterBottom>
                  Exported Files
                </Typography>
                {loading ? (
                  <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
                    <CircularProgress />
                  </Box>
                ) : exportedFiles.length > 0 ? (
                  <>
                    <List sx={{ maxHeight: 300, overflow: 'auto', mb: 2 }}>
                      {exportedFiles.map((file) => (
                        <ListItem key={file.id} disablePadding>
                          <ListItemButton onClick={() => fetchFileData(file.id)}>
                            <MuiListItemText
                              primary={file.name}
                              secondary={`Records: ${file.record_count}`}
                            />
                          </ListItemButton>
                        </ListItem>
                      ))}
                    </List>

                    {selectedFileData.length > 0 && (
                      <Box sx={{ mt: 2 }}>
                        <Typography variant="subtitle1" gutterBottom>
                          File Data Preview
                        </Typography>
                        <DataGrid
                          rows={selectedFileData}
                          columns={Object.keys(selectedFileData[0] || {})
                            .filter(key => key !== 'id')
                            .map(key => ({
                              field: key,
                              headerName: key.charAt(0).toUpperCase() + key.slice(1),
                              width: 150,
                              flex: 1
                            }))}
                          pageSize={5}
                          rowsPerPageOptions={[5, 10, 20]}
                          autoHeight
                          disableSelectionOnClick
                        />
                      </Box>
                    )}
                  </>
                ) : (
                  <Typography color="text.secondary">
                    No exported files available
                  </Typography>
                )}
              </Paper>
            </Grid>
          </Grid>
        )}
      </Paper>
    </Box>
  );
}

export default DataIngestion; 