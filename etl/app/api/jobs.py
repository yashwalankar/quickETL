from flask import Blueprint, request, jsonify
from datetime import datetime
import logging
from models import db
from models.jobs import Job
from models.runs import JobRun

logger = logging.getLogger(__name__)
jobs_bp = Blueprint('jobs', __name__)

# This will be injected by the main app
scheduler_service = None

def set_scheduler_service(scheduler):
    """Set the scheduler service instance"""
    global scheduler_service
    scheduler_service = scheduler

@jobs_bp.route('/jobs', methods=['GET'])
def get_jobs():
    """Get all jobs"""
    try:
        jobs = Job.query.all()
        return jsonify([job.to_dict() for job in jobs])
    except Exception as e:
        logger.error(f"Failed to get jobs: {str(e)}")
        return jsonify({'error': str(e)}), 500

@jobs_bp.route('/jobs', methods=['POST'])
def create_job():
    """Create a new job"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['name', 'script_path', 'cron_expression']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Create job
        job = Job(
            name=data['name'],
            description=data.get('description', ''),
            script_path=data['script_path'],
            cron_expression=data['cron_expression'],
            enabled=data.get('enabled', True),
            config=data.get('config', {})
        )
        
        db.session.add(job)
        db.session.commit()
        
        # Schedule the job if enabled
        if job.enabled and scheduler_service:
            scheduler_service.schedule_job(job)
        
        logger.info(f"Created job: {job.name}")
        
        return jsonify({
            'message': 'Job created successfully',
            'job_id': job.id,
            'job': job.to_dict()
        }), 201
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to create job: {str(e)}")
        return jsonify({'error': str(e)}), 400

@jobs_bp.route('/jobs/<int:job_id>', methods=['PUT'])
def update_job(job_id):
    """Update a job"""
    try:
        job = Job.query.get_or_404(job_id)
        data = request.get_json()
        
        job.name = data.get('name', job.name)
        job.description = data.get('description', job.description)
        job.script_path = data.get('script_path', job.script_path)
        job.cron_expression = data.get('cron_expression', job.cron_expression)
        job.enabled = data.get('enabled', job.enabled)
        job.config = data.get('config', job.config)
        job.updated_at = datetime.utcnow()
        
        db.session.commit()
        
        # Reschedule the job
        if scheduler_service:
            scheduler_service.schedule_job(job)
        
        logger.info(f"Updated job: {job.name}")
        
        return jsonify({
            'message': 'Job updated successfully',
            'job': job.to_dict()
        })
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to update job {job_id}: {str(e)}")
        return jsonify({'error': str(e)}), 400

@jobs_bp.route('/jobs/<int:job_id>', methods=['DELETE'])
def delete_job(job_id):
    """Delete a job"""
    try:
        job = Job.query.get_or_404(job_id)
        job_name = job.name
        
        # Remove from scheduler
        if scheduler_service:
            scheduler_service.unschedule_job(job_id)
        
        db.session.delete(job)
        db.session.commit()
        
        logger.info(f"Deleted job: {job_name}")
        
        return jsonify({'message': 'Job deleted successfully'})
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to delete job {job_id}: {str(e)}")
        return jsonify({'error': str(e)}), 400

@jobs_bp.route('/jobs/<int:job_id>/run', methods=['POST'])
def run_job_now(job_id):
    """Run a job immediately"""
    try:
        job = Job.query.get_or_404(job_id)
        
        # Schedule immediate execution
        if scheduler_service:
            success = scheduler_service.schedule_manual_job(job_id, job.name)
            if success:
                return jsonify({'message': 'Job execution started'})
            else:
                return jsonify({'error': 'Failed to start job execution'}), 500
        else:
            return jsonify({'error': 'Scheduler service not available'}), 503
        
    except Exception as e:
        logger.error(f"Failed to run job {job_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500
