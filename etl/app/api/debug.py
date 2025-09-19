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
    

@debug_bp.route('/kill-running-jobs', methods=['POST'])
def kill_running_jobs():
    """Kill all currently running job executions"""
    try:
        if not scheduler_service:
            return jsonify({'error': 'Scheduler service not available'}), 500
        
        from models.job_run import JobRun
        import signal
        import psutil
        
        # Get all running job runs from database
        running_job_runs = JobRun.query.filter_by(status='running').all()
        
        killed_jobs = []
        failed_kills = []
        
        # Method 1: Remove running jobs from APScheduler
        scheduler_jobs = scheduler_service.scheduler.get_jobs()
        for job in scheduler_jobs:
            # Kill manual/immediate execution jobs
            if job.id.startswith('manual_job_'):
                try:
                    scheduler_service.scheduler.remove_job(job.id)
                    killed_jobs.append({
                        'type': 'manual_execution',
                        'scheduler_job_id': job.id,
                        'method': 'removed_from_scheduler'
                    })
                except Exception as e:
                    failed_kills.append({
                        'scheduler_job_id': job.id,
                        'error': str(e)
                    })
        
        # Method 2: Update database job runs to 'failed' status
        for job_run in running_job_runs:
            try:
                job_run.status = 'failed'
                job_run.error_message = 'Job execution killed by admin'
                job_run.completed_at = datetime.now()
                
                if job_run.started_at:
                    duration = (datetime.now() - job_run.started_at).total_seconds()
                    job_run.duration_seconds = int(duration)
                
                db.session.commit()
                
                killed_jobs.append({
                    'type': 'database_job_run',
                    'job_run_id': job_run.id,
                    'job_id': job_run.job_id,
                    'method': 'marked_as_failed'
                })
                
            except Exception as e:
                failed_kills.append({
                    'job_run_id': job_run.id,
                    'error': str(e)
                })
        
        # Method 3: Find and kill actual Python processes running ETL jobs
        killed_processes = []
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'environ']):
                if proc.info['name'] in ['python', 'python3']:
                    try:
                        # Get process details
                        environ = proc.info.get('environ', {})
                        cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                        
                        # Check if this is a job process using multiple methods
                        is_job_process = False
                        job_id_from_env = None
                        
                        # Method 1: Check environment variables (most reliable)
                        if environ.get('JOB_ID'):
                            is_job_process = True
                            job_id_from_env = environ.get('JOB_ID')
                        
                        # Method 2: Check if running a script from jobs directory
                        elif '/app/jobs/' in cmdline:
                            is_job_process = True
                        
                        # Method 3: Check for job-related environment variables
                        elif environ.get('JOB_NAME') or environ.get('JOB_CONFIG'):
                            is_job_process = True
                            job_id_from_env = environ.get('JOB_ID')
                        
                        if is_job_process:
                            # Kill the process
                            process = psutil.Process(proc.info['pid'])
                            process.terminate()  # Try graceful termination first
                            try:
                                process.wait(timeout=5)
                                kill_method = 'terminated'
                            except psutil.TimeoutExpired:
                                process.kill()  # Force kill if graceful doesn't work
                                kill_method = 'force_killed'
                            
                            killed_processes.append({
                                'pid': proc.info['pid'],
                                'cmdline': cmdline,
                                'job_id': job_id_from_env,
                                'job_name': environ.get('JOB_NAME'),
                                'method': kill_method
                            })
                            
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        # Process might have ended or we don't have permission
                        continue
                    except Exception as e:
                        failed_kills.append({
                            'pid': proc.info['pid'],
                            'error': str(e)
                        })
                        
        except Exception as e:
            logger.warning(f"Error during process enumeration: {e}")
        
        return jsonify({
            'message': 'Job kill operation completed',
            'summary': {
                'total_killed': len(killed_jobs) + len(killed_processes),
                'scheduler_jobs_removed': len([j for j in killed_jobs if j['type'] == 'manual_execution']),
                'database_runs_failed': len([j for j in killed_jobs if j['type'] == 'database_job_run']),
                'processes_killed': len(killed_processes),
                'failed_operations': len(failed_kills)
            },
            'details': {
                'killed_jobs': killed_jobs,
                'killed_processes': killed_processes,
                'failed_kills': failed_kills
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to kill running jobs: {str(e)}")
        return jsonify({'error': str(e)}), 500

@debug_bp.route('/kill-specific-job/<int:job_id>', methods=['POST'])
def kill_specific_job(job_id):
    """Kill a specific job's current execution"""
    try:
        if not scheduler_service:
            return jsonify({'error': 'Scheduler service not available'}), 500
            
        from models.job_run import JobRun
        import psutil
        
        killed_items = []
        failed_kills = []
        
        # 1. Remove any manual executions of this job from scheduler
        scheduler_jobs = scheduler_service.scheduler.get_jobs()
        for job in scheduler_jobs:
            if job.id.startswith(f'manual_job_{job_id}_'):
                try:
                    scheduler_service.scheduler.remove_job(job.id)
                    killed_items.append({
                        'type': 'scheduler_job',
                        'scheduler_job_id': job.id
                    })
                except Exception as e:
                    failed_kills.append({'scheduler_job_id': job.id, 'error': str(e)})
        
        # 2. Mark running job runs as failed
        running_job_runs = JobRun.query.filter_by(job_id=job_id, status='running').all()
        for job_run in running_job_runs:
            try:
                job_run.status = 'failed'
                job_run.error_message = 'Job execution killed by admin'
                job_run.completed_at = datetime.now()
                
                if job_run.started_at:
                    duration = (datetime.now() - job_run.started_at).total_seconds()
                    job_run.duration_seconds = int(duration)
                
                db.session.commit()
                
                killed_items.append({
                    'type': 'job_run',
                    'job_run_id': job_run.id
                })
                
            except Exception as e:
                failed_kills.append({'job_run_id': job_run.id, 'error': str(e)})
        
        # 3. Find and kill processes running this specific job
        killed_processes = []
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'environ']):
                if proc.info['name'] in ['python', 'python3']:
                    try:
                        # Get process details
                        environ = proc.info.get('environ', {})
                        cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                        
                        # Check if this process is running our specific job
                        is_target_job = False
                        
                        # Method 1: Check JOB_ID environment variable (most reliable)
                        if environ.get('JOB_ID') == str(job_id):
                            is_target_job = True
                        
                        # Method 2: Check if running job script AND has job-related env vars
                        elif '/app/jobs/' in cmdline and (environ.get('JOB_NAME') or environ.get('JOB_CONFIG')):
                            # Additional check to make sure it's the right job
                            if environ.get('JOB_ID') == str(job_id):
                                is_target_job = True
                        
                        if is_target_job:
                            # Kill the process
                            process = psutil.Process(proc.info['pid'])
                            process.terminate()  # Try graceful termination first
                            try:
                                process.wait(timeout=5)
                                kill_method = 'terminated'
                            except psutil.TimeoutExpired:
                                process.kill()  # Force kill if graceful doesn't work
                                kill_method = 'force_killed'
                            
                            killed_processes.append({
                                'pid': proc.info['pid'],
                                'cmdline': cmdline,
                                'job_id': environ.get('JOB_ID'),
                                'job_name': environ.get('JOB_NAME'),
                                'method': kill_method
                            })
                            
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        # Process might have ended or we don't have permission
                        continue
                    except Exception as e:
                        failed_kills.append({'pid': proc.info['pid'], 'error': str(e)})
                        
        except Exception as e:
            logger.warning(f"Error during process enumeration for job {job_id}: {e}")
        
        return jsonify({
            'message': f'Kill operation completed for job {job_id}',
            'summary': {
                'total_killed': len(killed_items) + len(killed_processes),
                'scheduler_jobs_removed': len([k for k in killed_items if k['type'] == 'scheduler_job']),
                'job_runs_failed': len([k for k in killed_items if k['type'] == 'job_run']),
                'processes_killed': len(killed_processes),
                'failed_operations': len(failed_kills)
            },
            'details': {
                'killed_items': killed_items,
                'killed_processes': killed_processes,
                'failed_kills': failed_kills
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to kill job {job_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500