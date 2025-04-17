from fastapi import APIRouter, HTTPException
from typing import List
from pydantic import BaseModel
import os
from config.settings import settings

router = APIRouter()

class FileInfo(BaseModel):
    id: int
    name: str
    path: str
    record_count: int

@router.get("/files", response_model=List[FileInfo])
async def get_files():
    try:
        files = []
        if not os.path.exists(settings.UPLOAD_DIR):
            return []
            
        for idx, filename in enumerate(sorted(os.listdir(settings.UPLOAD_DIR), reverse=True)):
            if filename.endswith('_export.csv'):
                file_path = os.path.join(settings.UPLOAD_DIR, filename)
                # Count lines in the file (excluding header)
                with open(file_path, 'r') as f:
                    record_count = sum(1 for line in f) - 1
                
                files.append({
                    "id": idx + 1,
                    "name": filename,
                    "path": file_path,
                    "record_count": record_count
                })
        
        return files
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/files/{file_id}")
async def get_file(file_id: int):
    try:
        files = await get_files()
        if not files or file_id > len(files):
            raise HTTPException(status_code=404, detail="File not found")
            
        file_info = files[file_id - 1]
        
        # Read the file and return its contents
        import pandas as pd
        df = pd.read_csv(file_info['path'])
        
        # Convert to list of dictionaries with id
        records = []
        for idx, row in df.iterrows():
            record = row.to_dict()
            record['id'] = idx + 1
            records.append(record)
        
        return {
            "file_info": file_info,
            "records": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 