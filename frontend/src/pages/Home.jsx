import { Typography, Paper, Box, Button } from '@mui/material';
import { useNavigate } from 'react-router-dom';
import StorageIcon from '@mui/icons-material/Storage';
import FileUploadIcon from '@mui/icons-material/FileUpload';

function Home() {
  const navigate = useNavigate();

  return (
    <Box sx={{ textAlign: 'center', py: 4 }}>
      <Typography variant="h3" component="h1" gutterBottom>
        Welcome to Data Ingestion Tool
      </Typography>
      <Typography variant="h6" color="text.secondary" paragraph>
        A powerful tool for bidirectional data ingestion between ClickHouse and Flat Files
      </Typography>
      
      <Box sx={{ display: 'flex', justifyContent: 'center', gap: 4, mt: 4 }}>
        <Paper
          sx={{
            p: 4,
            maxWidth: 300,
            textAlign: 'center',
            cursor: 'pointer',
            '&:hover': { bgcolor: 'action.hover' },
          }}
          onClick={() => navigate('/ingestion')}
        >
          <StorageIcon sx={{ fontSize: 60, color: 'primary.main', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            ClickHouse Integration
          </Typography>
          <Typography color="text.secondary">
            Connect to ClickHouse database and manage your data
          </Typography>
        </Paper>

        <Paper
          sx={{
            p: 4,
            maxWidth: 300,
            textAlign: 'center',
            cursor: 'pointer',
            '&:hover': { bgcolor: 'action.hover' },
          }}
          onClick={() => navigate('/ingestion')}
        >
          <FileUploadIcon sx={{ fontSize: 60, color: 'secondary.main', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            Flat File Management
          </Typography>
          <Typography color="text.secondary">
            Upload and process flat files with ease
          </Typography>
        </Paper>
      </Box>
    </Box>
  );
}

export default Home; 