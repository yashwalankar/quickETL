from datetime import datetime
from models import db

class JobRun(db.Model):
    """Job run model for execution tracking"""
    __tablename__ = 'job_runs'
    
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)
    status = db.Column(db.String(50), default='pending')
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
    output = db.Column(db.Text)
    error_message = db.Column(db.Text)
    duration_seconds = db.Column(db.Integer)
    
    def to_dict(self):
        """Convert job run to dictionary for API responses"""
        return {
            'id': self.id,
            'job_id': self.job_id,
            'status': self.status,
            'started_at': self.started_at.isoformat() + 'Z' if self.started_at else None,
            'completed_at': self.completed_at.isoformat() + 'Z' if self.completed_at else None,
            'duration_seconds': self.duration_seconds,
            'output': self.output,
            'error_message': self.error_message
        }
    
    @property
    def is_running(self):
        """Check if job run is currently running"""
        return self.status == 'running'
    
    @property
    def is_completed(self):
        """Check if job run is completed (success or failed)"""
        return self.status in ['success', 'failed']
    
    def __repr__(self):
        return f'<JobRun {self.id} - {self.status}>'
