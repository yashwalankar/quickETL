import os
import logging
import pandas as pd
import requests
from datetime import datetime
import uuid
from dataclasses import dataclass
from typing import Optional
from questdb.ingress import Sender, IngressError

logger = logging.getLogger(__name__)

@dataclass
class LoadToQuestResponse:
    """Response object for QuestDB load operations"""
    success: bool
    records_processed: int
    records_new: int
    records_updated: int
    table_name: str
    symbol: Optional[str] = None
    load_batch_id: Optional[str] = None
    source_file: Optional[str] = None
    error_message: Optional[str] = None
    date_range_start: Optional[datetime] = None
    date_range_end: Optional[datetime] = None
    
    @property
    def total_records(self) -> int:
        """Total records processed"""
        return self.records_processed
    
    @property
    def has_updates(self) -> bool:
        """Check if any records were updated"""
        return self.records_updated > 0
    
    @property
    def has_new_records(self) -> bool:
        """Check if any new records were added"""
        return self.records_new > 0
    
    def __str__(self) -> str:
        if self.success:
            return (f"LoadToQuestResponse(success=True, table='{self.table_name}', "
                   f"symbol='{self.symbol}', processed={self.records_processed}, "
                   f"new={self.records_new}, updated={self.records_updated})")
        else:
            return (f"LoadToQuestResponse(success=False, table='{self.table_name}', "
                   f"error='{self.error_message}')")

class QuestDBLoader:
    """
    QuestDB loader class for handling OHLCV data with duplicate counting
    """
    
    def __init__(self, host='localhost', port=9000):
        self.host = host
        self.port = port
        self.http_url = f"http://{host}:{port}/exec"
        
    def check_connection(self):
        """Test QuestDB connection"""
        try:
            response = requests.get(f"http://{self.host}:{self.port}/")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Cannot connect to QuestDB: {e}")
            return False
    
    def get_existing_records(self, symbol, start_date, end_date, table_name='ohlcv_yf'):
        """Get existing records for the date range and symbol from specified table"""
        query = f"""
        SELECT datetime, symbol, insert_count, first_inserted 
        FROM {table_name} 
        WHERE symbol = '{symbol}' 
        AND datetime >= '{start_date}' 
        AND datetime <= '{end_date}'
        """
        
        try:
            response = requests.get(self.http_url, params={'query': query})
            if response.status_code == 200:
                result = response.json()
                if result.get('dataset'):
                    existing = {}
                    for row in result['dataset']:
                        key = (pd.to_datetime(row[0]), row[1]) # (datetime, symbol)
                        existing[key] = {
                            'count': row[2], 
                            'first_inserted': row[3]
                        }
                    return existing
            return {}
        except Exception as e:
            logger.error(f"Error checking existing records: {e}")
            return {}
    
    def load_csv_to_questdb(self, csv_file, table_name='ohlcv_yf') -> LoadToQuestResponse:
        """Load CSV data to QuestDB with upsert logic and insert counting"""
        
        # Initialize response object with default values
        response = LoadToQuestResponse(
            success=False,
            records_processed=0,
            records_new=0,
            records_updated=0,
            table_name=table_name,
            source_file=os.path.basename(csv_file) if csv_file else None
        )
        
        if not os.path.exists(csv_file):
            response.error_message = f"CSV file not found: {csv_file}"
            logger.error(response.error_message)
            return response
            
        try:
            # Read CSV data
            logger.info(f"Reading CSV file: {csv_file}")
            df = pd.read_csv(csv_file)
            
            if df.empty:
                response.success = True  # Empty file is not an error
                response.error_message = "CSV file is empty"
                logger.warning(f"CSV file is empty: {csv_file}")
                return response
            
            # Clean and prepare data
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            symbol = df['Symbol'].iloc[0]
            response.symbol = symbol
            
            # Get date range for checking existing records
            start_date = df['Datetime'].min()
            end_date = df['Datetime'].max()
            response.date_range_start = start_date
            response.date_range_end = end_date
            
            logger.info(f"Processing {len(df)} records for {symbol} into table '{table_name}'")
            logger.info(f"Date range: {start_date} to {end_date}")
            
            # Check existing records in the specified table
            existing_records = self.get_existing_records(symbol, start_date, end_date, table_name)
            logger.info(f"Found {len(existing_records)} existing records in date range")
            
            # Generate batch info
            load_batch_id = f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
            response.load_batch_id = load_batch_id
            source_file = os.path.basename(csv_file)
            
            # Counters
            records_new = 0
            records_updated = 0
            records_processed = 0
            
            # Use QuestDB client for efficient loading
            try:
                with Sender('http', self.host, self.port) as sender:
                    
                    for _, row in df.iterrows():
                        datetime_val = row['Datetime']
                        symbol_val = row['Symbol']
                        key = (datetime_val, symbol_val)
                        
                        # Determine if this is new or update
                        if key in existing_records:
                            insert_count = existing_records[key]['count'] + 1
                            first_inserted = pd.to_datetime(existing_records[key]['first_inserted'])
                            records_updated += 1
                        else:
                            insert_count = 1
                            first_inserted = datetime.now()
                            records_new += 1
                        
                        # Send row to QuestDB with configurable table name
                        sender.row(
                            table_name,
                            symbols={'symbol': symbol_val},
                            columns={
                                'open': float(row['Open']),
                                'high': float(row['High']),
                                'low': float(row['Low']),
                                'close': float(row['Close']),
                                'volume': int(row['Volume']),
                                'insert_count': insert_count,
                                'first_inserted': first_inserted,
                                'last_updated': datetime.now(),
                                'load_batch_id': load_batch_id,
                                'source_file': source_file
                            },
                            at=datetime_val
                        )
                        
                        records_processed += 1
                    
                    # Flush all data
                    sender.flush()
                    
                # Update response with success values
                response.success = True
                response.records_processed = records_processed
                response.records_new = records_new
                response.records_updated = records_updated
                    
                logger.info(f"Successfully loaded {records_processed} records to table '{table_name}'")
                logger.info(f"New records: {records_new}, Updated records: {records_updated}")
                
                # Log load summary
                self.log_load_summary(load_batch_id, symbol, records_processed, 
                                    records_new, records_updated, source_file, "SUCCESS", table_name)
                
                return response
                
            except IngressError as e:
                response.error_message = f"QuestDB ingress error: {str(e)}"
                logger.error(response.error_message)
                self.log_load_summary(load_batch_id, symbol, 0, 0, 0, source_file, "FAILED", table_name)
                return response
                
        except Exception as e:
            response.error_message = f"Error loading CSV to QuestDB: {str(e)}"
            logger.error(response.error_message)
            return response
    
    def log_load_summary(self, load_id, symbol, records_processed, records_new, records_updated, source_file, status, target_table):
        """Log summary of the load operation"""
        try:
            with Sender('http', self.host, self.port) as sender:
                sender.row(
                    'load_summary',
                    columns={
                        'load_id': load_id,
                        'symbol': symbol,
                        'records_processed': records_processed,
                        'records_new': records_new,
                        'records_updated': records_updated,
                        'source_file': source_file,
                        'target_table': target_table,
                        'job_status': status
                    },
                    at=datetime.now()
                )
                sender.flush()
        except Exception as e:
            logger.warning(f"Could not log load summary: {e}")
    
    def get_table_stats(self, table_name):
        """Get statistics about the table data"""
        try:
            query = f"""
            SELECT symbol, count(*) as total_records, 
                   sum(insert_count) as total_load_attempts,
                   max(insert_count) as max_duplicates,
                   min(datetime) as earliest_date,
                   max(datetime) as latest_date,
                   max(last_updated) as last_load_time
            FROM {table_name} 
            GROUP BY symbol 
            ORDER BY symbol
            """
            
            response = requests.get(self.http_url, params={'query': query})
            if response.status_code == 200:
                result = response.json()
                return result.get('dataset', [])
            else:
                logger.error(f"Error fetching stats: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return []
    
    def get_load_history(self, limit=10, table_name=None):
        """Get recent load history, optionally filtered by target table"""
        try:
            base_query = """
            SELECT load_id, symbol, load_timestamp, records_processed, 
                   records_new, records_updated, source_file, target_table, job_status
            FROM load_summary 
            """
            
            if table_name:
                query = f"{base_query} WHERE target_table = '{table_name}' ORDER BY load_timestamp DESC LIMIT {limit}"
            else:
                query = f"{base_query} ORDER BY load_timestamp DESC LIMIT {limit}"
            
            response = requests.get(self.http_url, params={'query': query})
            if response.status_code == 200:
                result = response.json()
                return result.get('dataset', [])
            else:
                logger.error(f"Error fetching load history: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error getting load history: {e}")
            return []