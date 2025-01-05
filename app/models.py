# app/models.py
from datetime import datetime
from app import db

class Warehouse(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True, nullable=False)
    description = db.Column(db.String(256))
    scripts = db.relationship('Script', backref='warehouse', lazy=True)

class Script(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=False)
    filename = db.Column(db.String(128), nullable=False)
    warehouse_id = db.Column(db.Integer, db.ForeignKey('warehouse.id'), nullable=False)
    description = db.Column(db.String(256))
    jobs = db.relationship('Job', backref='script', lazy=True)

class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.String(36), unique=True, nullable=False)  # APScheduler job ID
    script_id = db.Column(db.Integer, db.ForeignKey('script.id'), nullable=False)
    cron_expression = db.Column(db.String(128))
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    executions = db.relationship('JobExecution', backref='job', lazy=True)

class JobExecution(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'))
    start_time = db.Column(db.DateTime, nullable=False)
    end_time = db.Column(db.DateTime)
    status = db.Column(db.String(20))  # 'running', 'completed', 'failed'
    error_message = db.Column(db.Text)