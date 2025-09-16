from flask import Blueprint, jsonify
import logging
from models import db
from models.job import Job

logger = logging.getLogger(__name__)
debug_bp = Blueprint('debug', __name__)

# This will be injected by the main app
scheduler_service = None

def set_scheduler_service(scheduler):
    """Set the scheduler service instance"""
    global scheduler_service
    scheduler_service = scheduler

@debug_bp.route('/refresh-schedules', methods=['POST'])
def refresh_all_schedules():
    """Debug endpoint to refresh all job schedules"""
    try:
        logger.info("Manual refresh of all job schedules requested")
        
        # Get all jobs
        all_jobs = Job.query.all()
        
        for job in all_jobs:
            logger.info(f"Refreshing schedule for job {job.id}: {job.name} (enabled: {job.enabled})")
            if scheduler_service:
                scheduler_service.schedule_job(job)
        
        return jsonify({
            'message': f'Refreshed schedules for {len(all_jobs)} jobs',
            'jobs_processed': len(all_jobs)
        })
        
    except Exception as e:
        logger.error(f"Failed to refresh schedules: {str(e)}")
        return jsonify({'error': str(e)}), 500

@debug_bp.route('/job-status', methods=['GET'])
def get_debug_job_status():
    """Debug endpoint to show detailed job status"""
    try:
        jobs = Job.query.all()
        job_details = []
        
        for job in jobs:
            # Check if job is in scheduler
            scheduled_job = None
            try:
                if scheduler_service:
                    scheduled_job = scheduler_service.scheduler.get_job(f"job_{job.id}")
            except:
                pass
            
            job_details.append({
                'id': job.id,
                'name': job.name,
                'enabled': job.enabled,
                'cron_expression': job.cron_expression,
                'next_run_at': job.next_run_at.isoformat() + 'Z' if job.next_run_at else None,
                'in_scheduler': scheduled_job is not None,
                'scheduler_next_run': scheduled_job.next_run_time.isoformat() if scheduled_job and scheduled_job.next_run_time else None
            })
        
        return jsonify({
            'jobs': job_details,
            'scheduler_running': scheduler_service.is_running if scheduler_service else False,
            'total_scheduled_jobs': len(scheduler_service.scheduler.get_jobs()) if scheduler_service else 0
        })
        
    except Exception as e:
        logger.error(f"Failed to get debug status: {str(e)}")
        return jsonify({'error': str(e)}), 500
