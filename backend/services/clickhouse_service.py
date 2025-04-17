from clickhouse_connect import get_client
from typing import List, Optional
import pandas as pd
from models.clickhouse import ConnectionConfig, TableInfo, ColumnInfo, QueryConfig
from config.settings import settings
import os
import re

class ClickHouseService:
    def __init__(self):
        self.client = None
        self.current_config = None

    async def connect(self, config: ConnectionConfig) -> List[TableInfo]:
        try:
            # Clean up the host value by removing any protocol prefix, port, and trailing slashes
            clean_host = re.sub(r'^https?://', '', config.host)
            clean_host = re.sub(r':\d+$', '', clean_host)  # Remove port if present
            clean_host = re.sub(r'/$', '', clean_host)
            
            # If database is empty, use 'default'
            database = config.database if config.database else 'default'
            
            # Ensure port is an integer
            port = int(config.port) if isinstance(config.port, str) else config.port
            
            self.client = get_client(
                host=clean_host,
                port=port,
                database=database,
                username=config.user,
                password=config.password,
                secure=config.secure,
                verify=config.verify
            )
            self.current_config = config
            
            # Get list of tables
            result = self.client.query("""
                SELECT 
                    name,
                    engine,
                    total_rows
                FROM system.tables 
                WHERE database = %(database)s
            """, parameters={'database': database})
            
            return [
                TableInfo(
                    name=table[0],
                    engine=table[1],
                    row_count=table[2]
                )
                for table in result.result_rows
            ]
        except Exception as e:
            raise Exception(f"Failed to connect to ClickHouse: {str(e)}")

    async def get_table_columns(self, table_name: str) -> List[ColumnInfo]:
        if not self.client:
            raise Exception("Not connected to ClickHouse")
            
        try:
            result = self.client.query(f"""
                SELECT 
                    name,
                    type,
                    default_kind,
                    default_expression
                FROM system.columns 
                WHERE database = %(database)s AND table = %(table)s
            """, parameters={
                'database': self.current_config.database,
                'table': table_name
            })
            
            return [
                ColumnInfo(
                    name=col[0],
                    type=col[1],
                    default_kind=col[2],
                    default_expression=col[3]
                )
                for col in result.result_rows
            ]
        except Exception as e:
            raise Exception(f"Failed to get columns: {str(e)}")

    async def export_to_flatfile(self, query_config: QueryConfig):
        if not self.client:
            raise Exception("Not connected to ClickHouse")
            
        try:
            # Execute query and get data
            result = self.client.query(query_config.query)
            
            # Convert to DataFrame
            df = pd.DataFrame(result.result_rows, columns=query_config.columns)
            
            # Create output directory if it doesn't exist
            os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
            
            # Save to CSV
            output_path = os.path.join(settings.UPLOAD_DIR, f"{query_config.table_name}_export.csv")
            df.to_csv(output_path, index=False)
            
            return {
                "record_count": len(df),
                "file_path": output_path
            }
        except Exception as e:
            raise Exception(f"Failed to export data: {str(e)}")

    async def import_from_flatfile(self, table_name: str, file_path: str, columns: List[str]):
        if not self.client:
            raise Exception("Not connected to ClickHouse")
            
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Select only specified columns
            df = df[columns]
            
            # Convert DataFrame to list of tuples for insertion
            data = [tuple(x) for x in df.to_numpy()]
            
            # Insert data into ClickHouse
            self.client.insert(
                table_name,
                data,
                column_names=columns
            )
            
            return {
                "record_count": len(data)
            }
        except Exception as e:
            raise Exception(f"Failed to import data: {str(e)}") 