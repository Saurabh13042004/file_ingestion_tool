import pandas as pd
from fastapi import UploadFile
import os
from typing import List
from models.flatfile import FileInfo, ColumnInfo, PreviewData
from config.settings import settings

class FlatFileService:
    def __init__(self):
        self.upload_dir = settings.UPLOAD_DIR
        os.makedirs(self.upload_dir, exist_ok=True)

    async def save_file(self, file: UploadFile) -> FileInfo:
        try:
            # Create file path
            file_path = os.path.join(self.upload_dir, file.filename)
            
            # Save file
            content = await file.read()
            with open(file_path, "wb") as f:
                f.write(content)
            
            # Read file info
            df = pd.read_csv(file_path)
            
            return FileInfo(
                filename=file.filename,
                file_path=file_path,
                row_count=len(df),
                column_count=len(df.columns)
            )
        except Exception as e:
            raise Exception(f"Failed to save file: {str(e)}")

    async def get_file_columns(self, filename: str) -> List[ColumnInfo]:
        try:
            file_path = os.path.join(self.upload_dir, filename)
            df = pd.read_csv(file_path)
            
            return [
                ColumnInfo(
                    name=col,
                    type=str(df[col].dtype)
                )
                for col in df.columns
            ]
        except Exception as e:
            raise Exception(f"Failed to get file columns: {str(e)}")

    async def preview_file(self, filename: str, limit: int = 100) -> PreviewData:
        try:
            file_path = os.path.join(self.upload_dir, filename)
            df = pd.read_csv(file_path)
            
            # Get total rows
            total_rows = len(df)
            
            # Get preview data
            preview_df = df.head(limit)
            
            return PreviewData(
                data=preview_df.to_dict(orient='records'),
                columns=list(df.columns),
                total_rows=total_rows
            )
        except Exception as e:
            raise Exception(f"Failed to preview file: {str(e)}") 