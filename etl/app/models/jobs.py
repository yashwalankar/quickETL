from datetime import datetime
from models import db

class Job(db.Model):
    """Job model for scheduled tasks"""
    __tablename__ = 'jobs'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    description = db.Column(db.Text)
    script_path = db.Column(db.String(255), nullable=False)
    cron_expression = db.Column(db.String(100), nullable=False)
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_run_at = db.Column(db.DateTime)
    next_run_at = db.Column(db.DateTime)
    config = db.Column(db.JSON, default={})
    
    # Relationships
    runs = db.relationship('JobRun', backref='job', lazy=True, cascade='all, delete-orphan')
    
    def to_dict(self):
        """Convert job to dictionary for API responses"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'script_path': self.script_path,
            'cron_expression': self.cron_expression,
            'enabled': self.enabled,
            'created_at': self.created_at.isoformat() + 'Z' if self.created_at else None,
            'updated_at': self.updated_at.isoformat() + 'Z' if self.updated_at else None,
            'last_run_at': self.last_run_at.isoformat() + 'Z' if self.last_run_at else None,
            'next_run_at': self.next_run_at.isoformat() + 'Z' if self.next_run_at else None,
            'config': self.config
        }
    
    def __repr__(self):
        return f'<Job {self.name}>'
