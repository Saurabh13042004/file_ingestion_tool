 

https://github.com/user-attachments/assets/eddae6fc-0740-40a4-8b15-f82ca04522af

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

## Project Structure

```
.
├── frontend/           # React + Vite frontend
├── backend/           # FastAPI backend
├── README.md         # Project documentation
└── prompts.txt       # AI tool prompts used
```

## Setup Instructions

### Backend Setup

1. Navigate to the backend directory:
   ```bash
   cd backend
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start the backend server:
   ```bash
   uvicorn main:app --reload
   ```

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

## Usage

1. Open your browser and navigate to `http://localhost:5173`
2. Select your data source (ClickHouse or Flat File)
3. Configure connection parameters
4. Select columns for ingestion
5. Start the ingestion process

## Testing

The application includes test cases for:
- Single ClickHouse table to Flat File ingestion
- Flat File to ClickHouse table ingestion
- Multi-table join ingestion (bonus)
- Connection and authentication error handling
- Data preview functionality

## License

MIT 
