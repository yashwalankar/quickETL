import os
import json 
import logging
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_stock_data(symbol, interval, period, auto_adjust=True, 
                       include_prepost=False, output_dir='/app/yfdata/stocks', 
                       custom_filename=None, job_id='unknown', save_stats=False):
    """Download stock data using Yahoo Finance"""
    
    print(f"Downloading {symbol} data...")
    print(f"Interval: {interval}")
    print(f"Period: {period}")
    print(f"Auto Adjust: {auto_adjust}")
    print(f"Include Pre/Post Market: {include_prepost}")
    
    # TODO: Warn about intraday limitations
    # intraday_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m']
    
    
    try:
        # Create ticker and download data
        ticker = yf.Ticker(symbol)
        data = ticker.history(
            period=period,
            interval=interval,
            auto_adjust=auto_adjust,
            prepost=include_prepost
        )
        
        if data.empty:
            print(f"No data found for {symbol}")
            logger.error(f"No data found for {symbol}")
            return None, None
        
        # Add symbol column
        data['Symbol'] = symbol
        data = data.drop(columns=['Capital Gains','Dividends','Stock Splits']) 
        
        # Reset index to make datetime a column
        data.reset_index(inplace=True)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp for when the API call was made
        if custom_filename:
            filename = custom_filename
        else:
            api_call_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{symbol}-yfinance-{interval}-{period}-{api_call_timestamp}.csv"
        
        # Ensure .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # Full path
        filepath = os.path.join(output_dir, filename)
        
        # Save to CSV
        data.to_csv(filepath, index=False)
        
        # Calculate file size
        file_size_kb = os.path.getsize(filepath) / 1024
        
        # Print results
        print(f"\nDownload completed!")
        print(f"Records: {len(data):,}")
        print(f"Date range: {data['Datetime'].iloc[0]} to {data['Datetime'].iloc[-1]}")
        print(f"Price range: ${data['Low'].min():.2f} - ${data['High'].max():.2f}")
        print(f"Saved to: {filepath}")
        print(f"File size: {file_size_kb:.1f} KB")
        
        # Show sample data
        print(f"\nSample data (first 3 rows):")
        sample_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol']
        available_cols = [col for col in sample_cols if col in data.columns]
        print(data[available_cols].head(3).to_string(index=False))
        
        # Log statistics
        logger.info(f"Downloaded {len(data)} records for {symbol}")
        logger.info(f"Date range: {data['Datetime'].iloc[0]} to {data['Datetime'].iloc[-1]}")
        logger.info(f"File saved: {filepath} ({file_size_kb:.1f} KB)")
        
        # Generate summary statistics
        stats = {
            'symbol': symbol,
            'records_count': len(data),
            'date_range_start': str(data['Datetime'].iloc[0]),
            'date_range_end': str(data['Datetime'].iloc[-1]),
            'price_low': float(data['Low'].min()),
            'price_high': float(data['High'].max()),
            'avg_volume': int(data['Volume'].mean()) if 'Volume' in data.columns else None,
            'file_path': filepath,
            'file_size_kb': round(file_size_kb, 1),
            'download_timestamp': datetime.now().isoformat(),
            'job_id': job_id,
            'interval': interval,
            'period': period
        }
        
        # Save stats to JSON if on 
        if save_stats:
            stats_filename = filepath.replace('.csv', '_stats.json')
            with open(stats_filename, 'w') as f:
                json.dump(stats, f, indent=2)
            
            print(f"Stats saved to: {stats_filename}")
            logger.info(f"Statistics saved to: {stats_filename}")
        
        return data, filepath
        
    except Exception as e:
        print(f" Error: {str(e)}")
        logger.error(f"Download error: {str(e)}")
        return None, None


def main():
    # Get job configuration from environment
    job_config = json.loads(os.getenv('JOB_CONFIG', '{}'))
    job_name = os.getenv('JOB_NAME', 'daily_report')
    job_id = os.getenv('JOB_ID', 'unknown')
    
    logger.info(f"Starting job {job_name} (ID: {job_id}) with config; {job_config}")

    # Extract configuration with defaults
    symbol = job_config.get('symbol', 'SPY').upper()
    interval = job_config.get('interval', '1d')
    period = job_config.get('period', '1y')
    auto_adjust = job_config.get('auto_adjust', True)
    include_prepost = job_config.get('include_prepost', False)
    output_dir = job_config.get('output_dir', '/app/yfdata/stocks')
    custom_filename = job_config.get('filename', None)
    save_stats = job_config.get('save_stats', False)

    try:
        # Download stock data
        data, filename = download_stock_data(
            symbol=symbol,
            interval=interval,
            period=period,
            auto_adjust=auto_adjust,
            include_prepost=include_prepost,
            output_dir=output_dir,
            custom_filename=custom_filename,
            job_id=job_id,
            save_stats=save_stats
        )
        
        if data is not None:
            logger.info("SPY OHLCV download job completed successfully")
        else:
            logger.error("SPY OHLCV download job failed")
            exit(1)
            
    except Exception as e:
        logger.error(f"SPY OHLCV download failed: {e}")
        print(f"ERROR: SPY OHLCV download failed - {e}")
        exit(1)




if __name__ == "__main__":
    main()
