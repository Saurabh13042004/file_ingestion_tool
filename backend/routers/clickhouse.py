from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from pydantic import BaseModel
from services.clickhouse_service import ClickHouseService
from models.clickhouse import (
    ConnectionConfig,
    TableInfo,
    ColumnInfo,
    QueryConfig
)

router = APIRouter()
clickhouse_service = ClickHouseService()

class ConnectionResponse(BaseModel):
    message: str
    tables: Optional[List[TableInfo]] = None

class ImportRequest(BaseModel):
    table_name: str
    file_path: str
    columns: List[str]

@router.post("/connect", response_model=ConnectionResponse)
async def connect_to_clickhouse(config: ConnectionConfig):
    try:
        tables = await clickhouse_service.connect(config)
        return {
            "message": "Successfully connected to ClickHouse",
            "tables": tables
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/tables/{table_name}/columns", response_model=List[ColumnInfo])
async def get_table_columns(table_name: str):
    try:
        return await clickhouse_service.get_table_columns(table_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/export")
async def export_to_flatfile(query_config: QueryConfig):
    try:
        result = await clickhouse_service.export_to_flatfile(query_config)
        return {
            "message": "Data exported successfully",
            "record_count": result.record_count,
            "file_path": result.file_path
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/import")
async def import_from_flatfile(request: ImportRequest):
    try:
        result = await clickhouse_service.import_from_flatfile(
            request.table_name,
            request.file_path,
            request.columns
        )
        return {
            "message": "Data imported successfully",
            "record_count": result.record_count
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 