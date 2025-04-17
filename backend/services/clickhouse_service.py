from clickhouse_connect import get_client
from typing import List, Optional, Dict, Any
import pandas as pd
from models.clickhouse import ClickHouseConfig, TableInfo, ColumnInfo, QueryConfig, JoinConditions, ExportResponse
from config.settings import settings
import os
import re
from fastapi import HTTPException
import tempfile

class ClickHouseService:
    def __init__(self):
        self.client = None
        self.current_config = None

    async def connect(self, config: ClickHouseConfig) -> List[TableInfo]:
        try:
            # Create client with provided config
            self.client = get_client(
                host=config.host,
                port=config.port,
                username=config.user,
                password=config.password,
                database=config.database
            )
            self.current_config = config
            
            # Get tables from all databases except system
            query = """
            SELECT concat(database, '.', name) as table_name, engine, total_rows
            FROM system.tables
            WHERE database NOT IN ('system', 'information_schema')
            ORDER BY table_name
            """
            result = self.client.query(query)
            return [
                TableInfo(
                    name=row[0],
                    engine=row[1],
                    row_count=row[2] if row[2] is not None else 0
                )
                for row in result.result_rows
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_table_columns(self, table_name: str) -> List[ColumnInfo]:
        if not self.client:
            raise Exception("Not connected to ClickHouse")
            
        try:
            # Split database and table name
            db_table = table_name.split('.')
            if len(db_table) != 2:
                raise Exception("Invalid table name format. Expected 'database.table'")
            
            database, table = db_table
            print(f"Getting columns for {database}.{table}")
            
            result = self.client.query("""
                SELECT 
                    name,
                    type,
                    default_kind,
                    default_expression
                FROM system.columns 
                WHERE database = %(database)s AND table = %(table)s
            """, parameters={
                'database': database,
                'table': table
            })
            
            print("Columns query result:", result.result_rows)
            
            columns = [
                ColumnInfo(
                    name=col[0],
                    type=col[1],
                    default_kind=col[2],
                    default_expression=col[3]
                )
                for col in result.result_rows
            ]
            
            print("Processed columns:", columns)
            return columns
            
        except Exception as e:
            print(f"Error in get_table_columns: {str(e)}")
            raise Exception(f"Failed to get columns: {str(e)}")

    async def export_to_flatfile(self, query_config: QueryConfig) -> Dict[str, Any]:
        if not self.client:
            raise Exception("Not connected to ClickHouse")
            
        try:
            print(f"Executing query: {query_config.query}")
            # Execute query and get data
            result = self.client.query(query_config.query)
            
            print(f"Query result: {len(result.result_rows)} rows")
            
            # Convert to DataFrame
            df = pd.DataFrame(result.result_rows, columns=query_config.columns)
            
            # Create output directory if it doesn't exist
            os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
            
            # Save to CSV
            output_path = os.path.join(settings.UPLOAD_DIR, f"{query_config.table_name}_export.csv")
            df.to_csv(output_path, index=False)
            
            # Convert records to list of dictionaries with id
            records = []
            for idx, row in enumerate(result.result_rows):
                record = dict(zip(query_config.columns, row))
                record['id'] = idx + 1  # Add 1-based id for each record
                records.append(record)
            
            return {
                "message": "Data exported successfully",
                "record_count": len(records),
                "file_path": output_path,
                "records": records
            }
        except Exception as e:
            print(f"Error in export_to_flatfile: {str(e)}")
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

    async def get_tables(self) -> List[TableInfo]:
        try:
            # Get tables from all databases except system
            query = """
            SELECT concat(database, '.', name) as table_name, engine, total_rows
            FROM system.tables
            WHERE database NOT IN ('system', 'information_schema')
            ORDER BY table_name
            """
            result = self.client.query(query)
            return [
                TableInfo(
                    name=row[0],
                    engine=row[1],
                    row_count=row[2] if row[2] is not None else 0
                )
                for row in result.result_rows
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_columns(self, table: str) -> List[ColumnInfo]:
        try:
            if not self.client:
                raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
                
            # Handle database.table format
            if '.' in table:
                database, table_name = table.split('.')
                query = f"""
                SELECT name, type, default_kind, default_expression
                FROM system.columns
                WHERE database = '{database}' AND table = '{table_name}'
                ORDER BY position
                """
            else:
                query = f"""
                SELECT name, type, default_kind, default_expression
                FROM system.columns
                WHERE table = '{table}'
                ORDER BY position
                """
            result = self.client.query(query)
            return [
                ColumnInfo(
                    name=row[0],
                    type=row[1],
                    default_kind=row[2],
                    default_expression=row[3]
                )
                for row in result.result_rows
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def export_data(self, table_name: str, columns: List[str], join_conditions: JoinConditions = None) -> ExportResponse:
        try:
            if not self.client:
                raise HTTPException(status_code=400, detail="Not connected to ClickHouse")
                
            if join_conditions:
                # Handle multi-table JOIN
                tables = join_conditions.tables
                join_type = join_conditions.type
                join_keys = join_conditions.keys
                
                if len(tables) < 2:
                    raise HTTPException(status_code=400, detail="At least two tables required for JOIN")
                
                # Build JOIN query
                select_columns = []
                for table in tables:
                    table_cols = await self.get_columns(table)
                    for col in table_cols:
                        if col.name in columns:
                            select_columns.append(f"{table}.{col.name}")
                
                join_clauses = []
                for i in range(1, len(tables)):
                    prev_table = tables[i-1]
                    curr_table = tables[i]
                    join_key = join_keys.get(f"{prev_table}_{curr_table}")
                    if not join_key:
                        raise HTTPException(status_code=400, detail=f"Missing join key for {prev_table} and {curr_table}")
                    join_clauses.append(f"{join_type} JOIN {curr_table} ON {prev_table}.{join_key} = {curr_table}.{join_key}")
                
                query = f"""
                SELECT {', '.join(select_columns)}
                FROM {tables[0]}
                {' '.join(join_clauses)}
                """
            else:
                # Single table query
                query = f"SELECT {', '.join(columns)} FROM {table_name} LIMIT 100"
            
            # Execute query and get results
            result = self.client.query(query)
            
            # Convert to list of dictionaries with IDs
            records = []
            for idx, row in enumerate(result.result_rows):
                record = {'id': idx + 1}  # Add 1-based ID
                for i, col in enumerate(columns):
                    # Convert date to string if it's a date object
                    value = row[i]
                    if hasattr(value, 'strftime'):
                        value = value.strftime('%Y-%m-%d')
                    record[col] = value
                records.append(record)
            
            # Create upload directory if it doesn't exist
            os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
            
            # Generate file path in upload directory
            file_path = os.path.join(settings.UPLOAD_DIR, f"{table_name.replace('.', '_')}_export.csv")
            
            # Save records to CSV file
            with open(file_path, 'w') as f:
                # Write header
                f.write(','.join(columns) + '\n')
                # Write data
                for record in records:
                    row = [str(record.get(col, '')) for col in columns]
                    f.write(','.join(row) + '\n')
            
            return ExportResponse(
                message=f"Successfully exported {len(records)} records",
                record_count=len(records),
                file_path=file_path,
                records=records
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) 