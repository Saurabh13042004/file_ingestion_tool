from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from routers import clickhouse, files
from config.settings import Settings
from services.clickhouse_service import ClickHouseService
from services.flatfile_service import FlatFileService
from models.clickhouse import ClickHouseConfig, TableInfo, ColumnInfo, JoinConditions, ExportResponse
from models.flatfile import FileInfo, ColumnInfo, PreviewData
from clickhouse_connect import get_client
from typing import List, Dict, Any
import os

app = FastAPI(title="Data Ingestion Tool", version="1.0.0")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(clickhouse.router, prefix="/api/clickhouse", tags=["clickhouse"])
app.include_router(files.router, prefix="/api", tags=["files"])

# Initialize services
clickhouse_client = None
clickhouse_service = None
flatfile_service = None

@app.post("/clickhouse/connect", response_model=TableInfo)
async def connect_clickhouse(config: ClickHouseConfig):
    global clickhouse_client, clickhouse_service, flatfile_service
    try:
        clickhouse_client = get_client(
            host=config.host,
            port=config.port,
            database=config.database,
            username=config.user,
            password=config.password,
            secure=config.secure,
            verify=config.verify
        )
        clickhouse_service = ClickHouseService(clickhouse_client)
        flatfile_service = FlatFileService(clickhouse_client)
        return {"status": "success", "message": "Connected to ClickHouse"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/clickhouse/tables", response_model=List[TableInfo])
async def get_tables():
    if not clickhouse_service:
        raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
    return await clickhouse_service.get_tables()

@app.get("/clickhouse/tables/{table}/columns", response_model=List[ColumnInfo])
async def get_columns(table: str):
    if not clickhouse_service:
        raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
    return await clickhouse_service.get_columns(table)

@app.post("/clickhouse/export", response_model=ExportResponse)
async def export_data(table_name: str = None, columns: List[str] = None, join_conditions: JoinConditions = None):
    if not clickhouse_service:
        raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
    return await clickhouse_service.export_data(table_name, columns, join_conditions)

@app.post("/flatfile/import")
async def import_flatfile(file_path: str, table_name: str, columns: list[str], delimiter: str = ','):
    if not flatfile_service:
        raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
    return await flatfile_service.import_to_clickhouse(file_path, table_name, columns, delimiter)

@app.get("/flatfile/files")
async def get_exported_files():
    if not flatfile_service:
        raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
    return await flatfile_service.get_exported_files()

@app.get("/flatfile/files/{file_id}")
async def get_file_data(file_id: str):
    if not flatfile_service:
        raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
    return await flatfile_service.get_file_data(file_id)

@app.get("/")
async def root():
    return {"message": "File Ingestion Tool API"}
