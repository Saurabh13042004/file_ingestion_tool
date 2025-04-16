from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from clickhouse_driver import Client
import json
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ClickHouseConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    jwt_token: str

class ColumnSelection(BaseModel):
    columns: List[str]
    table_name: str

@app.post("/api/connect-clickhouse")
async def connect_clickhouse(config: ClickHouseConfig):
    try:
        client = Client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.jwt_token,
            secure=True
        )
        # Test connection
        client.execute("SELECT 1")
        return {"status": "success", "message": "Connected to ClickHouse successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/tables")
async def get_tables(config: ClickHouseConfig):
    try:
        client = Client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.jwt_token,
            secure=True
        )
        tables = client.execute("SHOW TABLES")
        return {"tables": [table[0] for table in tables]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/columns/{table_name}")
async def get_columns(table_name: str, config: ClickHouseConfig):
    try:
        client = Client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.jwt_token,
            secure=True
        )
        columns = client.execute(f"DESCRIBE TABLE {table_name}")
        return {"columns": [col[0] for col in columns]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/clickhouse-to-file")
async def clickhouse_to_file(
    config: ClickHouseConfig,
    selection: ColumnSelection,
    delimiter: str = Form(",")
):
    try:
        client = Client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.jwt_token,
            secure=True
        )
        
        columns_str = ", ".join(selection.columns)
        query = f"SELECT {columns_str} FROM {selection.table_name}"
        data = client.execute(query)
        
        df = pd.DataFrame(data, columns=selection.columns)
        output_file = f"output_{selection.table_name}.csv"
        df.to_csv(output_file, index=False, sep=delimiter)
        
        return {
            "status": "success",
            "message": f"Data exported to {output_file}",
            "record_count": len(data)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/file-to-clickhouse")
async def file_to_clickhouse(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    config: ClickHouseConfig = Form(...),
    delimiter: str = Form(",")
):
    try:
        # Read the uploaded file
        df = pd.read_csv(file.file, delimiter=delimiter)
        
        client = Client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.jwt_token,
            secure=True
        )
        
        # Create table if not exists
        columns = [f"{col} String" for col in df.columns]  # Default to String type
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        client.execute(create_table_query)
        
        # Insert data
        data = df.values.tolist()
        client.execute(f"INSERT INTO {table_name} VALUES", data)
        
        return {
            "status": "success",
            "message": f"Data imported to {table_name}",
            "record_count": len(data)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/preview/{table_name}")
async def preview_data(
    table_name: str,
    config: ClickHouseConfig,
    limit: int = 100
):
    try:
        client = Client(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.jwt_token,
            secure=True
        )
        
        data = client.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        columns = client.execute(f"DESCRIBE TABLE {table_name}")
        
        return {
            "columns": [col[0] for col in columns],
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 