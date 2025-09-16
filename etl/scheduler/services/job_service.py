from datetime import datetime
import logging
from models import db
from models.jobs import Job
from models.runs import JobRun

logger = logging.getLogger(__name__)

class JobService:
    """Service layer for job management"""
    
    @staticmethod
    def get_all_jobs():
        """Get all jobs"""
        return Job.query.all()
    
    @staticmethod
    def get_job_by_id(job_id):
        """Get job by ID"""
        return Job.query.get(job_id)
    
    @staticmethod
    def create_job(job_data):
        """Create a new job"""
        try:
            job = Job(
                name=job_data['name'],
                description=job_data.get('description', ''),
                script_path=job_data['script_path'],
                cron_expression=job_data['cron_expression'],
                enabled=job_data.get('enabled', True),
                config=job_data.get('config', {})
            )
            
            db.session.add(job)
            db.session.commit()
            
            logger.info(f"Created job: {job.name}")
            return job
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to create job: {str(e)}")
            raise e
    
    @staticmethod
    def update_job(job_id, job_data):
        """Update an existing job"""
        try:
            job = Job.query.get_or_404(job_id)
            
            job.name = job_data.get('name', job.name)
            job.description = job_data.get('description', job.description)
            job.script_path = job_data.get('script_path', job.script_path)
            job.cron_expression = job_data.get('cron_expression', job.cron_expression)
            job.enabled = job_data.get('enabled', job.enabled)
            job.config = job_data.get('config', job.config)
            job.updated_at = datetime.utcnow()
            
            db.session.commit()
            
            logger.info(f"Updated job: {job.name}")
            return job
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to update job {job_id}: {str(e)}")
            raise e
    
    @staticmethod
    def delete_job(job_id):
        """Delete a job"""
        try:
            job = Job.query.get_or_404(job_id)
            job_name = job.name
            
            db.session.delete(job)
            db.session.commit()
            
            logger.info(f"Deleted job: {job_name}")
            return True
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to delete job {job_id}: {str(e)}")
            raise e
    
    @staticmethod
    def get_job_statistics():
        """Get job statistics for dashboard"""
        total_jobs = Job.query.count()
        enabled_jobs = Job.query.filter_by(enabled=True).count()
        running_jobs = JobRun.query.filter_by(status='running').count()
        
        return {
            'total_jobs': total_jobs,
            'enabled_jobs': enabled_jobs,
            'running_jobs': running_jobs
        }
