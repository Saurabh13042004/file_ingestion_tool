 

https://github.com/user-attachments/assets/eddae6fc-0740-40a4-8b15-f82ca04522af



---

# ClickHouse & Flat File Data Ingestion Tool

A web-based application that facilitates bidirectional data ingestion between ClickHouse database and Flat Files.

## Features

- Bidirectional data flow (ClickHouse ↔ Flat File)
- JWT token-based authentication for ClickHouse
- Column selection for data ingestion
- Progress tracking and record count reporting
- Data preview functionality
- Multi-table join support (bonus feature)

## Tech Stack

### Frontend
- React + Vite  
- Tailwind CSS  
- Axios for API calls

### Backend
- FastAPI (Python)  
- ClickHouse-driver  
- Python JWT library  
- Pandas for data handling  
- Docker & Docker Compose

## Project Structure

```
.
├── frontend/           # React + Vite frontend
├── backend/            # FastAPI backend (Dockerized)
├── docker-compose.yml  # Docker Compose configuration
├── README.md           # Project documentation
└── prompts.txt         # AI tool prompts used
```

## Setup Instructions

### Backend Setup (Dockerized)

1. Ensure Docker and Docker Compose are installed:
   - [Download Docker](https://www.docker.com/get-started)

2. From the root project directory, build and run the containers:
   ```bash
   docker-compose up --build
   ```

3. The FastAPI backend will be available at `http://localhost:8000`

### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

4. Open your browser and navigate to `http://localhost:5173`

## Usage

1. Select your data source (ClickHouse or Flat File)  
2. Configure connection parameters  
3. Select columns for ingestion  
4. Start the ingestion process  

## Testing

The application includes test cases for:
- Single ClickHouse table to Flat File ingestion
- Flat File to ClickHouse table ingestion
- Multi-table join ingestion (bonus)
- Connection and authentication error handling
- Data preview functionality

## License

MIT

