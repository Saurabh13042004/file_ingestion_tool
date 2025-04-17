import clickhouse_connect
import os
import time

def init_sample_data():
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password=''
    )
    
    print("Connected to ClickHouse. Initializing sample data...")
    
    # Read and execute the SQL file
    with open('scripts/init_clickhouse.sql', 'r') as f:
        sql_commands = f.read()
    
    # Split the SQL file into individual commands
    commands = sql_commands.split(';')
    
    for command in commands:
        if command.strip():
            print(f"Executing: {command[:100]}...")
            client.command(command)
            # Add a small delay to avoid overwhelming the server
            time.sleep(1)
    
    print("Sample data initialization complete!")

if __name__ == "__main__":
    init_sample_data() 