import time
import logging
from models import db
from config import Config

logger = logging.getLogger(__name__)

def wait_for_database(app):
    """Wait for database to be ready"""
    with app.app_context():
        max_retries = Config.DB_RETRY_MAX
        for i in range(max_retries):
            try:
                with db.engine.connect() as conn:
                    conn.execute(db.text('SELECT 1'))
                logger.info("Database connection established")
                return True
            except Exception as e:
                if i == max_retries - 1:
                    logger.error(f"Failed to connect to database after {max_retries} retries")
                    raise e
                logger.info(f"Waiting for database... ({i+1}/{max_retries})")
                time.sleep(Config.DB_RETRY_INTERVAL)
        return False

def init_database(app):
    """Initialize database connection"""
    db.init_app(app)
    return wait_for_database(app)
