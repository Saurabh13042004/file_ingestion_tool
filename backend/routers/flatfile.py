from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import List
from services.flatfile_service import FlatFileService
from models.flatfile import FileInfo, ColumnInfo

router = APIRouter()
flatfile_service = FlatFileService()

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_info = await flatfile_service.save_file(file)
        return {
            "message": "File uploaded successfully",
            "file_info": file_info
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/columns/{filename}", response_model=List[ColumnInfo])
async def get_file_columns(filename: str):
    try:
        return await flatfile_service.get_file_columns(filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/preview/{filename}")
async def preview_file(filename: str, limit: int = 100):
    try:
        preview_data = await flatfile_service.preview_file(filename, limit)
        return {
            "data": preview_data.data,
            "columns": preview_data.columns,
            "total_rows": preview_data.total_rows,
            "row_count": preview_data.total_rows  # Added for frontend compatibility
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 