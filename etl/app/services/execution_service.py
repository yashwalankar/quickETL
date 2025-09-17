import os
import json
import logging
import subprocess
import sys
import traceback
from datetime import datetime
from models import db
from models.jobs import Job
from models.job_run import JobRun

logger = logging.getLogger(__name__)

class ExecutionService:
    """Service layer for job execution"""
    
    @staticmethod
    def execute_job(app, job_id):
        """Execute a job and log the results"""
        with app.app_context():  # Use the passed app, not current_app
            logger.info(f"Starting execution of job {job_id}")
            
            job = Job.query.get(job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                return
            
            # Create job run record
            job_run = JobRun(job_id=job_id, status='running')
            db.session.add(job_run)
            db.session.commit()
            logger.info(f"Created job run record with ID: {job_run.id}")
            
            try:
                start_time = datetime.utcnow()
                
                # Execute the script
                script_path = job.script_path
                if not os.path.exists(script_path):
                    raise FileNotFoundError(f"Script not found: {script_path}")
                
                # Pass job config as environment variable
                env = os.environ.copy()
                env['JOB_CONFIG'] = json.dumps(job.config)
                env['JOB_ID'] = str(job_id)
                env['JOB_NAME'] = job.name
                
                result = subprocess.run(
                    [sys.executable, script_path],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=3600  # 1 hour timeout
                )
                
                end_time = datetime.utcnow()
                duration = int((end_time - start_time).total_seconds())
                
                # Update job run record
                job_run.completed_at = end_time
                job_run.duration_seconds = duration
                job_run.output = result.stdout
                
                if result.returncode == 0:
                    job_run.status = 'success'
                    logger.info(f"Job {job_id} completed successfully")
                else:
                    job_run.status = 'failed'
                    job_run.error_message = result.stderr
                    logger.error(f"Job {job_id} failed: {result.stderr}")
                
                # Update job's last run time
                job.last_run_at = start_time
                if hasattr(app, 'scheduler_service') and job.enabled:
                    try:
                        scheduled_job = app.scheduler_service.scheduler.get_job(f"job_{job.id}")
                        if scheduled_job and scheduled_job.next_run_time:
                            job.next_run_at = scheduled_job.next_run_time.replace(tzinfo=None)
                    except Exception as e:
                            logger.warning(f"Failed to update next_run_at: {e}")
                
            except Exception as e:
                end_time = datetime.utcnow()
                duration = int((end_time - start_time).total_seconds()) if 'start_time' in locals() else 0
                
                job_run.completed_at = end_time
                job_run.duration_seconds = duration
                job_run.status = 'failed'
                job_run.error_message = str(e)
                
                logger.error(f"Job {job_id} failed with exception: {str(e)}")
                logger.error(traceback.format_exc())
            
            finally:
                db.session.commit()
                logger.info(f"Final commit completed for job run {job_run.id} with status: {job_run.status}")

    
    @staticmethod
    def get_job_runs(job_id, limit=50):
        """Get job run history"""
        logger.info(f"Fetching runs for job {job_id}")
        
        # Check if job exists
        job = Job.query.get(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            raise ValueError(f"Job {job_id} not found")
        
        runs = JobRun.query.filter_by(job_id=job_id).order_by(JobRun.started_at.desc()).limit(limit).all()
        
        logger.info(f"Found {len(runs)} runs for job {job_id}")
        return runs
