import logging
from datetime import datetime
from croniter import croniter
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import current_app
from models import db
from models.jobs import Job
from services.execution_service import ExecutionService

logger = logging.getLogger(__name__)

class SchedulerService:
    """Service layer for job scheduling"""
    
    def __init__(self, app):
        self.app = app
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        logger.info("Scheduler service initialized")
    
    def schedule_job(self, job):
        """Schedule a job with the scheduler"""
        try:
            # Always remove existing job first
            try:
                self.scheduler.remove_job(f"job_{job.id}")
                logger.info(f"Removed existing schedule for job {job.id}")
            except:
                pass  # Job wasn't scheduled, that's fine
            
            # Handle disabled jobs
            if not job.enabled:
                logger.info(f"Job {job.id} is disabled, clearing next_run_at")
                # Clear next_run_at since job is disabled
                with current_app.app_context():
                    job.next_run_at = None
                    db.session.commit()
                return
            
            # Calculate next run time using UTC
            logger.info(f"Calculating next run time for job {job.id} with cron: {job.cron_expression}")
            cron = croniter(job.cron_expression, datetime.utcnow())
            next_run = cron.get_next(datetime)
            
            logger.info(f"Calculated next run for job {job.id}: {next_run}")
            
            # Add job to scheduler
            
            self.scheduler.add_job(
                func=ExecutionService.execute_job,
                trigger=CronTrigger.from_crontab(job.cron_expression),
                args=[self.app, job.id, self.scheduler],
                id=f"job_{job.id}",
                name=job.name,
                replace_existing=True
            )
            
            # Update next run time in database
            with current_app.app_context():
                job.next_run_at = next_run
                db.session.commit()
            
            logger.info(f"Successfully scheduled job {job.id}: {job.name}, next run: {next_run}")
            
        except Exception as e:
            logger.error(f"Failed to schedule job {job.id}: {str(e)}")
            logger.error(f"Job details - enabled: {job.enabled}, cron: {job.cron_expression}")
            import traceback
            logger.error(traceback.format_exc())
    
    def unschedule_job(self, job_id):
        """Remove job from scheduler"""
        try:
            self.scheduler.remove_job(f"job_{job_id}")
            logger.info(f"Unscheduled job {job_id}")
        except Exception as e:
            logger.warning(f"Could not unschedule job {job_id}: {str(e)}")
    
    def schedule_manual_job(self, job_id, job_name):
        """Schedule a job to run immediately"""
        try:
            unique_id = f"manual_job_{job_id}_{datetime.now().timestamp()}"
            self.scheduler.add_job(
                func=ExecutionService.execute_job,
                args=[self.app, job_id, self.scheduler],
                id=unique_id,
                name=f"Manual run: {job_name}"
            )
            logger.info(f"Manually scheduled job: {job_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to manually schedule job {job_id}: {str(e)}")
            return False
    
    def load_existing_jobs(self):
        """Load and schedule existing jobs from database"""
        with current_app.app_context():
            logger.info("Loading existing jobs...")
            
            # First clean up disabled jobs
            disabled_jobs = Job.query.filter_by(enabled=False).all()
            for job in disabled_jobs:
                if job.next_run_at is not None:
                    logger.info(f"Clearing next_run_at for disabled job {job.id}: {job.name}")
                    job.next_run_at = None
            
            # Then load enabled jobs
            jobs = Job.query.filter_by(enabled=True).all()
            
            for job in jobs:
                logger.info(f"Loading job {job.id}: {job.name} (enabled: {job.enabled})")
                self.schedule_job(job)
            
            if disabled_jobs:
                db.session.commit()
                logger.info(f"Cleared next_run_at for {len(disabled_jobs)} disabled jobs")
            
            logger.info(f"Loaded {len(jobs)} enabled jobs")
    
    @property
    def is_running(self):
        """Check if scheduler is running"""
        return self.scheduler.running
    
    def shutdown(self):
        """Shutdown the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler shutdown completed")
