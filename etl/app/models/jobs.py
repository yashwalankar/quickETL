from flask import Blueprint, jsonify
import logging
from services.execution_service import ExecutionService

logger = logging.getLogger(__name__)
runs_bp = Blueprint('runs', __name__)

@runs_bp.route('/jobs/<int:job_id>/runs', methods=['GET'])
def get_job_runs(job_id):
    """Get job run history"""
    try:
        runs = ExecutionService.get_job_runs(job_id)
        return jsonify([run.to_dict() for run in runs])
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Failed to get job runs for {job_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500
