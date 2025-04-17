import clickhouse_connect
import os
import time
from datetime import datetime

def init_clickhouse():
    print("Starting ClickHouse initialization...")
    
    # Wait for ClickHouse to be fully ready
    time.sleep(5)
    
    try:
        # Connect to ClickHouse using HTTP port
        client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,  # HTTP port
            username='default',
            password=''
        )
        
        print("Successfully connected to ClickHouse")
        
        # Create database if it doesn't exist
        client.command('CREATE DATABASE IF NOT EXISTS uk')
        print("Created/verified 'uk' database")
        
        # Create table
        client.command("""
        CREATE TABLE IF NOT EXISTS uk.uk_price_paid
        (
            price UInt32,
            date Date,
            postcode1 String,
            postcode2 String,
            type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
            is_new UInt8,
            duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
            addr1 String,
            addr2 String,
            street String,
            locality String,
            town String,
            district String,
            county String,
            category UInt8
        )
        ENGINE = MergeTree
        ORDER BY (postcode1, postcode2, addr1, addr2)
        """)
        
        print("Created table uk.uk_price_paid")
        
        # Insert sample data with proper date conversion
        sample_data = [
            (100000, datetime.strptime('2023-01-01', '%Y-%m-%d').date(), 'SW1A', '1AA', 'flat', 1, 'leasehold', '10', 'Downing Street', 'Downing Street', 'Westminster', 'London', 'Greater London', 'London', 1),
            (250000, datetime.strptime('2023-02-15', '%Y-%m-%d').date(), 'SW1A', '2AA', 'terraced', 0, 'freehold', '11', 'Downing Street', 'Downing Street', 'Westminster', 'London', 'Greater London', 'London', 1),
            (500000, datetime.strptime('2023-03-20', '%Y-%m-%d').date(), 'SW1A', '3AA', 'detached', 1, 'freehold', '12', 'Downing Street', 'Downing Street', 'Westminster', 'London', 'Greater London', 'London', 1)
        ]
        
        client.insert('uk.uk_price_paid', sample_data, column_names=[
            'price', 'date', 'postcode1', 'postcode2', 'type', 'is_new', 'duration',
            'addr1', 'addr2', 'street', 'locality', 'town', 'district', 'county', 'category'
        ])
        
        print("Inserted sample data")
        
        # Verify data
        result = client.query('SELECT count() FROM uk.uk_price_paid')
        print(f"Total rows in uk.uk_price_paid: {result.result_rows[0][0]}")
        
        # List all tables
        tables = client.query("""
            SELECT database, name 
            FROM system.tables 
            WHERE database NOT IN ('system', 'information_schema')
            ORDER BY database, name
        """)
        print("Available tables:", tables.result_rows)
        
    except Exception as e:
        print(f"Error during initialization: {str(e)}")
        raise

if __name__ == '__main__':
    init_clickhouse() 