import os
import logging
import pandas as pd
import requests
from datetime import datetime
import uuid
from questdb.ingress import Sender, IngressError

logger = logging.getLogger(__name__)

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
    
    def get_existing_records(self, symbol, start_date, end_date):
        """Get existing records for the date range to handle duplicates"""
        query = f"""
        SELECT datetime, symbol, insert_count, first_inserted 
        FROM ohlcv_stocks 
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
                        key = (row[0], row[1])  # (datetime, symbol)
                        existing[key] = {
                            'count': row[2], 
                            'first_inserted': row[3]
                        }
                    return existing
            return {}
        except Exception as e:
            logger.error(f"Error checking existing records: {e}")
            return {}
    
    def load_csv_to_questdb(self, csv_file, table_name='ohlcv_stocks'):
        """Load CSV data to QuestDB with upsert logic and insert counting"""
        
        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return False, 0, 0, 0
            
        try:
            # Read CSV data
            logger.info(f"Reading CSV file: {csv_file}")
            df = pd.read_csv(csv_file)
            
            if df.empty:
                logger.warning(f"CSV file is empty: {csv_file}")
                return True, 0, 0, 0
            
            # Validate required columns
            required_cols = ['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in CSV: {missing_cols}")
                return False, 0, 0, 0
            
            # Clean and prepare data
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            symbol = df['Symbol'].iloc[0]
            
            # Get date range for checking existing records
            start_date = df['Datetime'].min()
            end_date = df['Datetime'].max()
            
            logger.info(f"Processing {len(df)} records for {symbol}")
            logger.info(f"Date range: {start_date} to {end_date}")
            
            # Check existing records
            existing_records = self.get_existing_records(symbol, start_date, end_date)
            logger.info(f"Found {len(existing_records)} existing records in date range")
            
            # Generate batch info
            load_batch_id = f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
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
                        key = (datetime_val.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), symbol_val)
                        
                        # Determine if this is new or update
                        if key in existing_records:
                            insert_count = existing_records[key]['count'] + 1
                            first_inserted = existing_records[key]['first_inserted']
                            records_updated += 1
                        else:
                            insert_count = 1
                            first_inserted = datetime.now()
                            records_new += 1
                        
                        # Send row to QuestDB
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
                    
                logger.info(f"Successfully loaded {records_processed} records")
                logger.info(f"New records: {records_new}, Updated records: {records_updated}")
                
                # Log load summary
                self.log_load_summary(load_batch_id, symbol, records_processed, 
                                    records_new, records_updated, source_file, "SUCCESS")
                
                return True, records_processed, records_new, records_updated
                
            except IngressError as e:
                logger.error(f"QuestDB ingress error: {e}")
                self.log_load_summary(load_batch_id, symbol, 0, 0, 0, source_file, "FAILED")
                return False, 0, 0, 0
                
        except Exception as e:
            logger.error(f"Error loading CSV to QuestDB: {e}")
            return False, 0, 0, 0
    
    def log_load_summary(self, load_id, symbol, records_processed, records_new, records_updated, source_file, status):
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
                        'job_status': status
                    },
                    at=datetime.now()
                )
                sender.flush()
        except Exception as e:
            logger.warning(f"Could not log load summary: {e}")
    
    def get_table_stats(self, table_name='ohlcv_stocks'):
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
    
    def get_load_history(self, limit=10):
        """Get recent load history"""
        try:
            query = f"""
            SELECT load_id, symbol, load_timestamp, records_processed, 
                   records_new, records_updated, source_file, job_status
            FROM load_summary 
            ORDER BY load_timestamp DESC 
            LIMIT {limit}
            """
            
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
    