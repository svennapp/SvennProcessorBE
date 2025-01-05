# run.py
from app import create_app, db
from app.models import Warehouse, Script, Job, JobExecution  # Import your models

app = create_app()

@app.shell_context_processor
def make_shell_context():
    return {
        'db': db,
        'Warehouse': Warehouse,
        'Script': Script,
        'Job': Job,
        'JobExecution': JobExecution
    }