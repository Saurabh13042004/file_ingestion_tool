from pydantic import BaseModel
from typing import List, Dict, Any

class FileInfo(BaseModel):
    filename: str
    file_path: str
    row_count: int
    column_count: int

class ColumnInfo(BaseModel):
    name: str
    type: str

class PreviewData(BaseModel):
    data: List[Dict[str, Any]]
    columns: List[str]
    total_rows: int 