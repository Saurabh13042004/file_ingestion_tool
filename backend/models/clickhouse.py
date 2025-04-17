from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class ClickHouseConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool = False
    verify: bool = True

class TableInfo(BaseModel):
    name: str
    engine: str
    row_count: int

class ColumnInfo(BaseModel):
    name: str
    type: str
    default_kind: Optional[str] = None
    default_expression: Optional[str] = None

class JoinConditions(BaseModel):
    tables: List[str]
    type: str
    keys: Dict[str, str]

class ExportRequest(BaseModel):
    table_name: str
    columns: List[str]
    query: Optional[str] = None
    limit: Optional[int] = 100

class Record(BaseModel):
    id: int
    price: float
    date: str
    postcode1: str

class ExportResponse(BaseModel):
    message: str
    record_count: int
    file_path: str
    records: List[Dict[str, Any]]

class QueryConfig(BaseModel):
    table_name: str
    columns: List[str]
    query: str
    limit: Optional[int] = None

class ExportResult(BaseModel):
    record_count: int
    file_path: str

class ImportResult(BaseModel):
    record_count: int 