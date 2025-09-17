from flask import Blueprint, jsonify
import logging
from models.jobs import Job

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

@debug_bp.route('/compare-db-vs-scheduler', methods=['GET'])
def compare_db_vs_scheduler_jobs():
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

@debug_bp.route('/jobs-in-scheduler', methods=['GET'])
def get_jobs_in_scheduler():
    """Get all jobs currently in the APScheduler instance"""
    try:
        if not scheduler_service:
            return jsonify({'error': 'Scheduler service not available'}), 500
        
        # Get all jobs from APScheduler
        scheduled_jobs = scheduler_service.scheduler.get_jobs()
        
        job_list = []
        for job in scheduled_jobs:
            job_info = {
                'id': job.id,
                'name': job.name,
                'func': f"{job.func.__module__}.{job.func.__name__}" if job.func else None,
                'trigger': str(job.trigger),
                'trigger_type': type(job.trigger).__name__,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'pending': job.pending,
                'coalesce': job.coalesce,
                'max_instances': job.max_instances,
                'misfire_grace_time': job.misfire_grace_time,
                'args': list(job.args) if job.args else [],
                'kwargs': dict(job.kwargs) if job.kwargs else {}
            }
            job_list.append(job_info)
        
        # Sort by next run time
        job_list.sort(key=lambda x: x['next_run_time'] or '9999-12-31')
        
        return jsonify({
            'scheduler_running': scheduler_service.is_running,
            'total_jobs': len(job_list),
            'jobs': job_list
        })
        
    except Exception as e:
        logger.error(f"Failed to get scheduler jobs: {str(e)}")
        return jsonify({'error': str(e)}), 500