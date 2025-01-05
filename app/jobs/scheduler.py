# app/jobs/scheduler.py
import sys
import importlib.util
from datetime import datetime

from apscheduler.jobstores.base import JobLookupError
from flask import current_app
from app import db, scheduler
from app.models import Job, JobExecution
from pathlib import Path
from app.jobs.utils.logging_config import setup_script_logging
from app.jobs.common.database_manager import DatabaseManager

logger = setup_script_logging('scheduler')


def import_warehouse_script(warehouse: str, script_name: str):
    """
    Dynamically import a warehouse script

    Args:
        warehouse: Name of the warehouse (e.g., 'byggmakker')
        script_name: Name of the script file without .py extension
    """
    try:
        # Build the import path using Path for proper cross-platform handling
        script_path = Path(current_app.config['JOBS_FOLDER']) / 'warehouse_scripts' / warehouse / f"{script_name}.py"
        script_path = script_path.resolve()  # Resolve to absolute path

        logger.info(f"Attempting to import script from: {script_path}")

        if not script_path.exists():
            raise FileNotFoundError(f"Script file not found: {script_path}")

        # Import the module
        spec = importlib.util.spec_from_file_location(
            f"{warehouse}_{script_name}",
            str(script_path)
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    except Exception as e:
        logger.error(f"Failed to import script {script_name} from {warehouse}: {e}")
        raise


def execute_script(job_id: int):
    """Execute a script and log its execution"""
    from flask import current_app
    logger.info(f"Starting script execution for job ID: {job_id}")
    execution = None
    db_manager = None

    # Get the Flask app instance
    from app import create_app
    app = create_app()

    # Run everything within the application context
    with app.app_context():
        try:
            logger.info("Created application context")

            # Get job details
            job = Job.query.get(job_id)
            if not job:
                raise ValueError(f"Job {job_id} not found")
            logger.info(f"Found job: {job.script.filename}")

            # Parse warehouse and script name from filename
            parts = job.script.filename.split('/')
            if len(parts) != 2:
                raise ValueError(f"Invalid script filename format: {job.script.filename}")

            warehouse, script_file = parts[0], parts[1]
            script_name = script_file.replace('.py', '')
            logger.info(f"Parsed script info - warehouse: {warehouse}, script: {script_name}")

            # Create execution record
            execution = JobExecution(
                job_id=job.id,
                start_time=datetime.utcnow(),
                status='running'
            )
            db.session.add(execution)
            db.session.commit()
            logger.info(f"Created execution record with ID: {execution.id}")

            # Initialize DatabaseManager with root .env file
            env_path = Path(app.root_path).parent / '.env'
            logger.info(f"Looking for .env file at: {env_path}")

            if not env_path.exists():
                raise FileNotFoundError(f"Environment file not found: {env_path}")

            db_manager = DatabaseManager(env_path)
            logger.info("DatabaseManager initialized successfully")

            # Import and execute script
            script_module = import_warehouse_script(warehouse, script_name)
            logger.info(f"Successfully imported script: {job.script.filename}")

            if hasattr(script_module, 'main'):
                logger.info("Found main() function in script, executing...")
                script_module.main()
                logger.info("Script execution completed successfully")
                execution.status = 'completed'
            else:
                raise AttributeError("Script has no main() function")

        except Exception as e:
            error_msg = f"Script execution failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if execution:
                execution.status = 'failed'
                execution.error_message = error_msg
            raise

        finally:
            if execution:
                execution.end_time = datetime.utcnow()
                try:
                    db.session.commit()
                    logger.info(f"Execution record updated. Status: {execution.status}")
                except Exception as e:
                    logger.error(f"Failed to update execution record: {e}")
                    db.session.rollback()

            if db_manager:
                logger.info("Closing database connections")
                db_manager.close_all_connections()


def add_job(script_id: int, cron_expression: str) -> Job:
    """
    Add a new scheduled job

    Args:
        script_id: ID of the script to schedule
        cron_expression: Cron expression for scheduling

    Returns:
        Job: Created job instance
    """
    try:
        from app.models import Script
        script = Script.query.get(script_id)
        if not script:
            raise ValueError("Script not found")

        # Validate script exists in warehouse_scripts
        parts = script.filename.split('/')
        if len(parts) != 2:
            raise ValueError(f"Invalid script filename format: {script.filename}")

        warehouse, script_file = parts[0], parts[1]
        script_path = Path(current_app.config['JOBS_FOLDER']) / 'warehouse_scripts' / warehouse / script_file
        if not script_path.exists():
            raise FileNotFoundError(f"Script file not found: {script_path}")

        # Add job to APScheduler
        job = scheduler.add_job(
            func=execute_script,
            trigger='cron',
            args=[script_id],
            id=f'script_{script_id}',
            **parse_cron_expression(cron_expression)
        )

        # Create job record in database
        db_job = Job(
            job_id=job.id,
            script_id=script_id,
            cron_expression=cron_expression
        )
        db.session.add(db_job)
        db.session.commit()
        logger.info(f"Successfully added job for script: {script.filename}")

        return db_job

    except Exception as e:
        logger.error(f"Failed to add job: {e}", exc_info=True)
        raise


def remove_job(job_id: int):
    """
    Remove a scheduled job

    Args:
        job_id: ID of the job to remove
    """
    try:
        job = Job.query.get(job_id)
        if not job:
            raise ValueError("Job not found")

        # Try to remove from APScheduler if it exists
        try:
            scheduler.remove_job(job.job_id)
        except JobLookupError:
            logger.warning(f"Job {job.job_id} not found in APScheduler - might have been lost after restart")
            # Continue with database removal even if APScheduler job is not found

        # Remove from database
        db.session.delete(job)
        db.session.commit()
        logger.info(f"Successfully removed job ID: {job_id}")

    except Exception as e:
        logger.error(f"Failed to remove job: {e}", exc_info=True)
        db.session.rollback()
        raise


def parse_cron_expression(expression: str) -> dict:
    """
    Parse cron expression into APScheduler kwargs

    Args:
        expression: Cron expression string

    Returns:
        dict: APScheduler compatible keyword arguments
    """
    parts = expression.split()
    if len(parts) != 5:
        raise ValueError("Invalid cron expression. Expected format: 'minute hour day month day_of_week'")

    return {
        'minute': parts[0],
        'hour': parts[1],
        'day': parts[2],
        'month': parts[3],
        'day_of_week': parts[4]
    }


def toggle_job(job_id: int) -> bool:
    """
    Toggle a job's enabled status (pause/resume)

    Args:
        job_id: Database ID of the job to toggle

    Returns:
        bool: True if job is now enabled, False if disabled

    Raises:
        ValueError: If job not found
        Exception: If toggle operation fails
    """
    try:
        # Get job from database
        job = Job.query.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        # Toggle enabled status
        job.enabled = not job.enabled

        if job.enabled:
            # Resume job in APScheduler
            try:
                scheduler.resume_job(job.job_id)
            except JobLookupError:
                # If job not in scheduler (e.g., after restart), recreate it
                scheduler.add_job(
                    func=execute_script,
                    trigger='cron',
                    args=[job_id],
                    id=job.job_id,
                    **parse_cron_expression(job.cron_expression)
                )
        else:
            # Pause job in APScheduler
            try:
                scheduler.pause_job(job.job_id)
            except JobLookupError:
                # If job not found in scheduler, that's okay - it's already paused
                pass

        db.session.commit()
        return job.enabled

    except Exception as e:
        logger.error(f"Failed to toggle job {job_id}: {e}")
        db.session.rollback()
        raise

def update_job(job_id: int, cron_expression: str = None) -> Job:
    """
    Update an existing scheduled job

    Args:
        job_id: ID of the job to update
        cron_expression: New cron expression for scheduling (optional)

    Returns:
        Job: Updated job instance

    Raises:
        ValueError: If job not found or invalid cron expression
        Exception: If update operation fails
    """
    try:
        # Get job from database
        job = Job.query.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        if cron_expression:
            # Validate cron expression by attempting to parse it
            try:
                cron_kwargs = parse_cron_expression(cron_expression)
            except ValueError as e:
                raise ValueError(f"Invalid cron expression: {e}")

            # Update job in APScheduler
            try:
                scheduler.reschedule_job(
                    job.job_id,
                    trigger='cron',
                    **cron_kwargs
                )
            except JobLookupError:
                # If job not in scheduler (e.g., after restart), recreate it
                scheduler.add_job(
                    func=execute_script,
                    trigger='cron',
                    args=[job_id],
                    id=job.job_id,
                    **cron_kwargs
                )

            # Update cron expression in database
            job.cron_expression = cron_expression

        db.session.commit()
        return job

    except Exception as e:
        logger.error(f"Failed to update job {job_id}: {e}")
        db.session.rollback()
        raise