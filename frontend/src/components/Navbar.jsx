import React from 'react';
import { AppBar, Toolbar, Typography, Button, Box } from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import StorageIcon from '@mui/icons-material/Storage';

const Navbar = () => {
  return (
    <AppBar position="static">
      <Toolbar>
        <StorageIcon sx={{ mr: 2 }} />
        <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
          File Ingestion Tool
        </Typography>
        <Box>
          <Button color="inherit" component={RouterLink} to="/">
            Home
          </Button>
          <Button color="inherit" component={RouterLink} to="/ingestion">
            Data Ingestion
          </Button>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

export default Navbar; 