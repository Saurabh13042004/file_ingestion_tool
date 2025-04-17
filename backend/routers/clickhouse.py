from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Extra
from services.clickhouse_service import ClickHouseService
from models.clickhouse import ClickHouseConfig, TableInfo, ColumnInfo, QueryConfig, JoinConditions, ExportResponse, ExportRequest
from clickhouse_connect import get_client

router = APIRouter()
clickhouse_service = ClickHouseService()

class ConnectionResponse(BaseModel):
    message: str
    tables: Optional[List[TableInfo]] = None

class ImportRequest(BaseModel):
    table_name: str
    file_path: str
    columns: List[str]

class Record(BaseModel):
    id: int
    # Allow any additional fields
    class Config:
        extra = Extra.allow

class ExportResponse(BaseModel):
    message: str
    record_count: int
    file_path: str
    records: List[Record]

@router.post("/connect", response_model=List[TableInfo])
async def connect_clickhouse(config: ClickHouseConfig):
    try:
        return await clickhouse_service.connect(config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tables", response_model=List[TableInfo])
async def get_tables():
    try:
        return await clickhouse_service.get_tables()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tables/{table}/columns", response_model=List[ColumnInfo])
async def get_columns(table: str):
    try:
        return await clickhouse_service.get_columns(table)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/export", response_model=ExportResponse)
async def export_data(request: ExportRequest):
    try:
        return await clickhouse_service.export_data(
            table_name=request.table_name,
            columns=request.columns,
            join_conditions=None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/import")
async def import_from_flatfile(request: ImportRequest):
    try:
        result = await ClickHouseService().import_from_flatfile(
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