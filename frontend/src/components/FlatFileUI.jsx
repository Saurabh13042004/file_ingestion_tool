import React, { useState, useEffect } from 'react';
import { DataGrid } from '@mui/x-data-grid';
import { Box, Typography, Paper, CircularProgress } from '@mui/material';
import axios from 'axios';

const FlatFileUI = () => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchExportedData();
    }, []);

    const fetchExportedData = async () => {
        try {
            setLoading(true);
            setError(null);
            
            // Get the list of exported files
            const response = await axios.get('http://localhost:8000/api/files');
            const files = response.data;
            
            if (files.length > 0) {
                // Get the most recent file
                const latestFile = files[0];
                const fileResponse = await axios.get(`http://localhost:8000/api/files/${latestFile.id}`);
                setData(fileResponse.data.records);
            }
        } catch (err) {
            setError('Failed to load exported data');
            console.error('Error fetching data:', err);
        } finally {
            setLoading(false);
        }
    };

    if (loading) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <CircularProgress />
            </Box>
        );
    }

    if (error) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <Typography color="error">{error}</Typography>
            </Box>
        );
    }

    if (data.length === 0) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <Typography>No exported data available</Typography>
            </Box>
        );
    }

    // Generate columns from the first row of data
    const columns = Object.keys(data[0])
        .filter(key => key !== 'id') // Exclude id from columns
        .map(key => ({
            field: key,
            headerName: key.charAt(0).toUpperCase() + key.slice(1),
            width: 150,
            editable: false,
        }));

    return (
        <Paper elevation={3} sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
                Exported Data
            </Typography>
            <Box sx={{ height: 400, width: '100%' }}>
                <DataGrid
                    rows={data}
                    columns={columns}
                    pageSize={5}
                    rowsPerPageOptions={[5, 10, 25]}
                    checkboxSelection
                    disableSelectionOnClick
                    experimentalFeatures={{ newEditingApi: true }}
                />
            </Box>
        </Paper>
    );
};

export default FlatFileUI; 