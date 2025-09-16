import os
import json 
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Get job configuration from environment
    job_config = json.loads(os.getenv('JOB_CONFIG', '{}'))
    job_name = os.getenv('JOB_NAME', 'daily_report')
    job_id = os.getenv('JOB_ID', 'unknown')
    
    logger.info(f"Starting job {job_name} (ID: {job_id}) with config; {job_config}")


if __name__ == "__main__":
    main()
