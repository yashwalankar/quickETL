"""
finds csv files in data_dir and loads it into table_name table in questdb

Example job_config JSON:
{
    "data_dir": "/custom/data/path",     // Directory containing CSV files (default: "/app/data")
    "symbol_pattern": "AAPL",           // Symbol pattern to match CSV files: '*' for all, or specific symbol like 'SPY' (default: "*")
    "table_name": "stock_data",         // Target table name in QuestDB (default: "NotDefined") #REQUIRED
    "only_latest_csv": false,           // Process only the most recent CSV file per symbol (default: true)
    "questdb_host": "localhost",        // QuestDB host address (default: inherited from global config)
    "questdb_port": 9009                // QuestDB port number (default: inherited from global config)
}
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
import glob
import uuid
import pandas as pd
from questdb_loader import QuestDBLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

""" 
Find csv files from a directory based on certain pattern
symbol_pattern}-yfinance-*-*_*.csv
"""
def find_csv(data_dir, symbol_pattern='*',latest = True):
    if not os.path.exists(data_dir):
        logger.error(f"Directory does not exist: {data_dir}")
        return []

    today = datetime.now().strftime('%Y%m%d')
    pattern = f"{data_dir}/{symbol_pattern}-yfinance-*-*_*.csv"

    files = glob.glob(pattern)
    
    if not files:
        print(f"No files found match pattern {pattern} for {today}")
        logger.error("No csv files found")
        return []

    if latest:
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest CSV file: {latest_file}")
        return [latest_file]
    else:
        return files 


def main():

    ## Getting Config 
    # From ENV Variables 

    job_config = json.loads(os.getenv('JOB_CONFIG', '{}'))
    job_name = os.getenv('JOB_NAME', 'questdb_loader')
    job_id = os.getenv('JOB_ID', str(uuid.uuid4())[:8])

    questdb_host = os.getenv('QUESTDB_URL','questdb')
    questdb_port = os.getenv('QUESTDB_PORT',9000)


    logger.info(f"Starting job {job_name} (ID: {job_id}) with config: {job_config}")

    # parse job config if provided
    data_dir = job_config.get('data_dir', '/app/data/stocks')
    symbol_pattern = job_config.get('symbol_pattern', '*')  # or specific symbol like 'SPY'
    table_name = job_config.get('table_name', 'NotDefined')
    only_latest_csv = job_config.get('only_latest_csv', True)

    questdb_host = job_config.get('questdb_host', questdb_host) #If different
    questdb_port = job_config.get('questdb_port', questdb_port) #If different

    if table_name == "NotDefined":
        logger.error("Table Name not defined")
        exit(1)
    
    logger.info(f"Running csv_to_questdb loader with config: "
           f"data_dir={data_dir}, symbol_pattern='{symbol_pattern}', "
           f"table_name='{table_name}', only_latest_csv={only_latest_csv}, "
           f"questdb_host='{questdb_host}', questdb_port={questdb_port}")

    logger.info(f"Running csv_to_questdb loader with ")
    #Re-write variables if passed through job config  
    questdb_host = job_config.get('questdb_host', questdb_host)
    questdb_port = job_config.get('questdb_port', questdb_port)

    ## Initialize QuestDB 
    logger.info(f"Connecting to QuestDB at {questdb_host}:{questdb_port}")
    loader = QuestDBLoader(questdb_host, questdb_port)

    ## Find CSV files to load 
    files = find_csv(data_dir=data_dir,latest=only_latest_csv)
    
    logger.info(f"Found csv files: {files}")
    print(files)

    ## Load CSV to QuestDB
    for file in files:
        load_response =loader.load_csv_to_questdb(file, table_name)
        logger.info(load_response)
    
    load_history = loader.get_load_history(limit = len(files), table_name=table_name)

    if not load_history:
        logger.warning("Load history not updated after load")
    else:
        logger.info("Load history updated")
        for ld in load_history:
            logger.info(ld)
            print(ld)



if __name__ == "__main__":
    main()