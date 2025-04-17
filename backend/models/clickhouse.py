from pydantic import BaseModel
from typing import List, Optional

class ConnectionConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: Optional[str] = None
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