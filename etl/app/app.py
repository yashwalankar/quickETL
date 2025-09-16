#!/usr/bin/env python3
"""
PROPERLY Refactored ETL Job Scheduler
Uses separate models/, services/, and api/ files - NO redundant code!
This app.py is small and just wires everything together.
"""

import os
import logging
import time
from pathlib import Path

from flask import Flask, render_template
from flask_cors import CORS

# =============================================================================
# CONFIGURATION
# =============================================================================
class Config:
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://user:pass@localhost:5432/scheduler_db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key')
    DEBUG = os.getenv('FLASK_DEBUG', 'true').lower() == 'true'
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# =============================================================================
# LOGGING SETUP
# =============================================================================
def setup_logging():
    Path('logs').mkdir(exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/app.log'),
            logging.StreamHandler()
        ]
    )
    # Suppress development server warning for cleaner logs
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    return logging.getLogger(__name__)

logger = setup_logging()

# =============================================================================
# APPLICATION FACTORY
# =============================================================================
def create_app():
    """Application factory that wires everything together"""
    
    # Create Flask app
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = Config.DATABASE_URL
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = Config.SQLALCHEMY_TRACK_MODIFICATIONS
    app.config['SECRET_KEY'] = Config.SECRET_KEY
    
    CORS(app)
    
    # Initialize database
    from models import db
    db.init_app(app)
    
    # Import models to ensure they're registered
    from models.job import Job
    from models.job_run import JobRun
    
    # Initialize scheduler service
    from services.scheduler_service import SchedulerService
    scheduler_service = SchedulerService(app)
    
    # Import API blueprints
    from api.jobs import jobs_bp, set_scheduler_service as set_jobs_scheduler
    from api.runs import runs_bp
    from api.system import system_bp, set_scheduler_service as set_system_scheduler
    from api.debug import debug_bp, set_scheduler_service as set_debug_scheduler
    
    # Inject scheduler service into blueprints that need it
    set_jobs_scheduler(scheduler_service)
    set_system_scheduler(scheduler_service)
    set_debug_scheduler(scheduler_service)
    
    # Register blueprints
    app.register_blueprint(jobs_bp, url_prefix='/api')
    app.register_blueprint(runs_bp, url_prefix='/api')
    app.register_blueprint(system_bp, url_prefix='/api')
    app.register_blueprint(debug_bp, url_prefix='/api/debug')
    
    # Main route
    @app.route('/')
    def index():
        return render_template('index.html')
    
    # Database utilities
    def wait_for_database():
        max_retries = 30
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
                time.sleep(2)
        return False
    
    # Store scheduler service reference for shutdown
    app.scheduler_service = scheduler_service
    
    # Initialize database and load jobs
    with app.app_context():
        wait_for_database()
        scheduler_service.load_existing_jobs()
    
    return app

# =============================================================================
# MAIN APPLICATION
# =============================================================================
if __name__ == '__main__':
    app = create_app()
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=Config.DEBUG)
    except KeyboardInterrupt:
        print("\\nShutting down gracefully...")
    finally:
        if hasattr(app, 'scheduler_service'):
            app.scheduler_service.shutdown()
