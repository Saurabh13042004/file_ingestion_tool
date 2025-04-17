import pandas as pd
from fastapi import UploadFile, HTTPException
import os
from typing import List, Dict, Any
from models.flatfile import FileInfo, ColumnInfo, PreviewData
from config.settings import settings
from clickhouse_connect import get_client
from models.clickhouse import ClickHouseConfig, TableInfo, ColumnInfo, JoinConditions, ExportResponse

class FlatFileService:
    def __init__(self, client):
        self.client = client
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

    async def import_from_flatfile(self, table_name: str, file_path: str, columns: List[str]) -> Dict[str, Any]:
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Select only the specified columns
            if columns:
                df = df[columns]
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Insert data into ClickHouse
            self.client.insert(table_name, records)
            
            return {
                "status": "success",
                "message": f"Successfully imported {len(records)} records",
                "record_count": len(records)
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def export_to_flatfile(self, table_name: str, columns: List[str], file_path: str) -> Dict[str, Any]:
        try:
            # Build the query
            query = f"SELECT {', '.join(columns)} FROM {table_name}"
            
            # Execute query and get results
            result = self.client.query(query)
            
            # Convert to DataFrame
            df = pd.DataFrame(result.result_rows, columns=columns)
            
            # Save to CSV
            df.to_csv(file_path, index=False)
            
            return {
                "status": "success",
                "message": f"Successfully exported {len(df)} records",
                "record_count": len(df),
                "file_path": file_path
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) 