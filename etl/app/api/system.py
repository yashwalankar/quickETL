from flask import Blueprint, jsonify
import logging
from models import db
from models.jobs import Job
from models.runs import JobRun

logger = logging.getLogger(__name__)
system_bp = Blueprint('system', __name__)

# This will be injected by the main app
scheduler_service = None

def set_scheduler_service(scheduler):
    """Set the scheduler service instance"""
    global scheduler_service
    scheduler_service = scheduler

@system_bp.route('/status', methods=['GET'])
def get_status():
    """Get system status"""
    try:
        total_jobs = Job.query.count()
        enabled_jobs = Job.query.filter_by(enabled=True).count()
        running_jobs = JobRun.query.filter_by(status='running').count()
        
        return jsonify({
            'total_jobs': total_jobs,
            'enabled_jobs': enabled_jobs,
            'running_jobs': running_jobs,
            'scheduler_running': scheduler_service.is_running if scheduler_service else False
        })
        
    except Exception as e:
        logger.error(f"Failed to get system status: {str(e)}")
        return jsonify({'error': str(e)}), 500
