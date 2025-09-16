import os
import json
import logging
from datetime import datetime, timedelta
import glob
import uuid
from questdb_loader import QuestDBLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_latest_csv(data_dir='../yfdata/stocks', symbol_pattern='*'):
    """Find the most recent CSV file for today's date"""
    
    # Look for today's files first
    today = datetime.now().strftime('%Y%m%d')
    pattern = f"{data_dir}/{symbol_pattern}-yfinance-*-{today}_*.csv"
    
    files = glob.glob(pattern)
    
    if not files:
        # Fall back to yesterday's files if today's not found
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        pattern = f"{data_dir}/{symbol_pattern}-yfinance-*-{yesterday}_*.csv"
        files = glob.glob(pattern)
    
    if not files:
        # Look for any recent files in the last 7 days
        for days_back in range(2, 8):
            date_str = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
            pattern = f"{data_dir}/{symbol_pattern}-yfinance-*-{date_str}_*.csv"
            files = glob.glob(pattern)
            if files:
                break
    
    if files:
        # Return the most recent file
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest CSV file: {latest_file}")
        return latest_file
    else:
        logger.error(f"No CSV files found in {data_dir}")
        return None
    
def main():
    print(find_latest_csv())

if __name__ == "__main__":
    main()