# app/api/routes.py
import os
import re
from datetime import datetime, timedelta
from flask import jsonify, request
from app import db
from app.api import bp
from app.models import Warehouse
from app.models import Script
from flask import current_app
from app.jobs.scheduler import add_job, remove_job, execute_script
from app.models import Job, JobExecution
from app.jobs.scheduler import toggle_job
from app.jobs.scheduler import update_job




@bp.route('/warehouses', methods=['GET'])
def get_warehouses():
    """Get all warehouses"""
    warehouses = Warehouse.query.all()
    return jsonify([{
        'id': w.id,
        'name': w.name,
        'description': w.description
    } for w in warehouses])


@bp.route('/warehouses/<int:id>', methods=['GET'])
def get_warehouse(id):
    """Get a specific warehouse by ID"""
    warehouse = Warehouse.query.get_or_404(id)
    return jsonify({
        'id': warehouse.id,
        'name': warehouse.name,
        'description': warehouse.description
    })


@bp.route('/warehouses', methods=['POST'])
def create_warehouse():
    """Create a new warehouse"""
    data = request.get_json() or {}

    if 'name' not in data:
        return jsonify({'error': 'Name is required'}), 400

    if Warehouse.query.filter_by(name=data['name']).first():
        return jsonify({'error': 'Warehouse name already exists'}), 400

    warehouse = Warehouse(
        name=data['name'],
        description=data.get('description', '')
    )

    db.session.add(warehouse)
    db.session.commit()

    return jsonify({
        'id': warehouse.id,
        'name': warehouse.name,
        'description': warehouse.description
    }), 201


@bp.route('/warehouses/<int:id>', methods=['PUT'])
def update_warehouse(id):
    """Update an existing warehouse"""
    warehouse = Warehouse.query.get_or_404(id)
    data = request.get_json() or {}

    if 'name' in data:
        if data['name'] != warehouse.name and \
                Warehouse.query.filter_by(name=data['name']).first():
            return jsonify({'error': 'Warehouse name already exists'}), 400
        warehouse.name = data['name']

    if 'description' in data:
        warehouse.description = data['description']

    db.session.commit()

    return jsonify({
        'id': warehouse.id,
        'name': warehouse.name,
        'description': warehouse.description
    })


@bp.route('/warehouses/<int:id>', methods=['DELETE'])
def delete_warehouse(id):
    """Delete a warehouse"""
    warehouse = Warehouse.query.get_or_404(id)
    db.session.delete(warehouse)
    db.session.commit()
    return '', 204


@bp.route('/warehouses/<int:warehouse_id>/scripts', methods=['GET'])
def get_warehouse_scripts(warehouse_id):
    """Get all scripts for a specific warehouse"""
    # First verify warehouse exists
    warehouse = Warehouse.query.get_or_404(warehouse_id)

    scripts = Script.query.filter_by(warehouse_id=warehouse_id).all()
    return jsonify([{
        'id': script.id,
        'name': script.name,
        'filename': script.filename,
        'description': script.description
    } for script in scripts])


@bp.route('/scripts/<int:id>', methods=['GET'])
def get_script(id):
    """Get a specific script by ID"""
    script = Script.query.get_or_404(id)
    return jsonify({
        'id': script.id,
        'name': script.name,
        'filename': script.filename,
        'warehouse_id': script.warehouse_id,
        'description': script.description
    })


@bp.route('/warehouses/<int:warehouse_id>/scripts', methods=['POST'])
def create_script(warehouse_id):
    """Add a new script to a warehouse"""
    # Verify warehouse exists
    warehouse = Warehouse.query.get_or_404(warehouse_id)

    data = request.get_json() or {}

    if not all(k in data for k in ('name', 'filename')):
        return jsonify({'error': 'Must include name and filename'}), 400

    # Verify the script file exists in the jobs folder
    script_path = os.path.join(current_app.config['JOBS_FOLDER'], data['filename'])
    if not os.path.isfile(script_path):
        return jsonify({'error': 'Script file does not exist in jobs folder'}), 400

    script = Script(
        name=data['name'],
        filename=data['filename'],
        description=data.get('description', ''),
        warehouse_id=warehouse_id
    )

    db.session.add(script)
    db.session.commit()

    return jsonify({
        'id': script.id,
        'name': script.name,
        'filename': script.filename,
        'warehouse_id': warehouse_id,
        'description': script.description
    }), 201


@bp.route('/scripts/<int:id>', methods=['PUT'])
def update_script(id):
    """Update details of a script"""
    script = Script.query.get_or_404(id)
    data = request.get_json() or {}

    if 'name' in data:
        script.name = data['name']

    if 'description' in data:
        script.description = data['description']

    if 'filename' in data:
        # Verify the new script file exists
        script_path = os.path.join(current_app.config['JOBS_FOLDER'], data['filename'])
        if not os.path.isfile(script_path):
            return jsonify({'error': 'Script file does not exist in jobs folder'}), 400
        script.filename = data['filename']

    db.session.commit()

    return jsonify({
        'id': script.id,
        'name': script.name,
        'filename': script.filename,
        'warehouse_id': script.warehouse_id,
        'description': script.description
    })


@bp.route('/scripts/<int:id>', methods=['DELETE'])
def delete_script(id):
    """Remove a script"""
    script = Script.query.get_or_404(id)

    # Check if script has any associated jobs before deletion
    if script.jobs:
        return jsonify({
            'error': 'Cannot delete script that has associated jobs. Delete jobs first.'
        }), 400

    db.session.delete(script)
    db.session.commit()

    return '', 204


@bp.route('/scripts/<int:script_id>/logs', methods=['GET'])
def get_script_logs(script_id):
    """Get logs for a specific script"""
    try:
        # Verify script exists
        script = Script.query.get_or_404(script_id)
        current_app.logger.info(f"Fetching logs for script: {script.name}")

        def normalize_script_name(name):
            """Normalize script name for comparison"""
            # First, remove 'Processor' suffix if it exists
            name = name.replace('Processor', '')

            # Remove any leading/trailing dashes or spaces
            name = name.strip('- ')

            # Handle CamelCase by inserting underscores
            name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)

            # Convert to lowercase and normalize spaces/underscores
            name = name.lower().replace(' ', '_')

            # Remove duplicate underscores and trim
            name = re.sub('_+', '_', name).strip('_')

            return name

        # Get query parameters for filtering
        hours = request.args.get('hours', type=int, default=24)
        log_level = request.args.get('level', type=str)

        # Calculate the time threshold
        time_threshold = datetime.utcnow() - timedelta(hours=hours)

        # Path to log file
        log_file = os.path.join(current_app.root_path, 'logs', 'script_executions.log')

        if not os.path.exists(log_file):
            current_app.logger.error(f"Log file not found at: {log_file}")
            return jsonify({'error': 'Log file not found'}), 404

        # Normalize the script name we're looking for
        script_name_normalized = normalize_script_name(script.name)
        current_app.logger.info(f"Looking for logs matching normalized name: {script_name_normalized}")

        # Read and parse log file
        logs = []
        current_log_entry = None
        possible_matches = set()  # Keep track of all script names we see

        with open(log_file, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    line = line.strip()
                    if not line:  # Skip empty lines
                        continue

                    # Try to identify if this is a new log entry
                    if len(line) > 19:  # Minimum length for timestamp
                        try:
                            # Parse timestamp
                            timestamp = datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')

                            # If we have a previous entry, save it
                            if current_log_entry:
                                logs.append(current_log_entry)

                            # Parse new log entry
                            remainder = line[20:].strip()  # Get everything after timestamp
                            parts = remainder.split(' - ', 2)  # Split into max 3 parts

                            if len(parts) >= 2:  # We need at least script_name and level
                                log_script_name = parts[0].strip('- ')  # Remove any leading/trailing dashes
                                level = parts[1].strip()
                                message = parts[2].strip() if len(parts) > 2 else ""

                                # Keep track of original script names for debugging
                                possible_matches.add(log_script_name)

                                # Normalize the script name from logs for comparison
                                log_script_normalized = normalize_script_name(log_script_name)

                                # Debug first few entries
                                if line_num <= 5:
                                    current_app.logger.debug(
                                        f"Line {line_num}:\n"
                                        f"  Original log name: '{log_script_name}'\n"
                                        f"  Normalized log name: '{log_script_normalized}'\n"
                                        f"  Looking for: '{script_name_normalized}'\n"
                                        f"  Match? {script_name_normalized == log_script_normalized}"
                                    )

                                # Check for match
                                if script_name_normalized == log_script_normalized:
                                    current_log_entry = {
                                        'timestamp': timestamp.isoformat(),
                                        'level': level,
                                        'message': message
                                    }
                                else:
                                    current_log_entry = None
                            else:
                                current_log_entry = None
                        except ValueError:
                            # Not a timestamp line, append to current message if exists
                            if current_log_entry:
                                current_log_entry['message'] += '\n' + line
                    elif current_log_entry:
                        # Append this line to the current message
                        current_log_entry['message'] += '\n' + line

                except Exception as e:
                    current_app.logger.error(f"Error parsing line {line_num}: {str(e)}")
                    continue

        # Add the last log entry if exists
        if current_log_entry:
            logs.append(current_log_entry)

        # Filter logs by time threshold
        logs = [log for log in logs if datetime.fromisoformat(log['timestamp']) >= time_threshold]

        # Filter by log level if specified
        if log_level:
            logs = [log for log in logs if log['level'].upper() == log_level.upper()]

        current_app.logger.info(f"Found {len(logs)} log entries for script {script.name}")
        current_app.logger.info(f"All script names found in logs (before normalization): {sorted(possible_matches)}")

        if logs:
            current_app.logger.info(f"Sample log entry: {logs[0]}")

        # Sort logs by timestamp (newest first)
        logs.sort(key=lambda x: x['timestamp'], reverse=True)

        return jsonify(logs)

    except Exception as e:
        current_app.logger.error(f"Error fetching logs: {str(e)}")
        return jsonify({'error': str(e)}), 500


# Error handlers
@bp.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not found'}), 404


@bp.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return jsonify({'error': 'Internal server error'}), 500


@bp.route('/jobs', methods=['GET'])
def get_jobs():
    """List all scheduled jobs"""
    jobs = Job.query.all()
    return jsonify([{
        'id': job.id,
        'job_id': job.job_id,
        'script_id': job.script_id,
        'cron_expression': job.cron_expression,
        'enabled': job.enabled,
        'created_at': job.created_at.isoformat()
    } for job in jobs])


@bp.route('/jobs', methods=['POST'])
def create_job():
    """Schedule a new job"""
    data = request.get_json() or {}

    if not all(k in data for k in ('script_id', 'cron_expression')):
        return jsonify({'error': 'Must include script_id and cron_expression'}), 400

    try:
        job = add_job(data['script_id'], data['cron_expression'])
        return jsonify({
            'id': job.id,
            'job_id': job.job_id,
            'script_id': job.script_id,
            'cron_expression': job.cron_expression,
            'enabled': job.enabled,
            'created_at': job.created_at.isoformat()
        }), 201
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Failed to create job: {str(e)}'}), 500


@bp.route('/jobs/<int:id>', methods=['DELETE'])
def delete_job(id):
    """Delete a scheduled job"""
    try:
        remove_job(id)
        return '', 204
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': f'Failed to delete job: {str(e)}'}), 500


@bp.route('/jobs/<int:id>/toggle', methods=['POST'])
def toggle_job_status(id):
    """Toggle a job's enabled status (pause/resume)"""
    try:
        enabled = toggle_job(id)
        return jsonify({
            'message': f"Job {'resumed' if enabled else 'paused'} successfully",
            'enabled': enabled
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': f'Failed to toggle job: {str(e)}'}), 500


@bp.route('/jobs/<int:id>', methods=['PUT'])
def update_job_route(id):
    """Update a scheduled job"""
    data = request.get_json() or {}

    try:
        job = update_job(
            job_id=id,
            cron_expression=data.get('cron_expression')
        )

        return jsonify({
            'id': job.id,
            'job_id': job.job_id,
            'script_id': job.script_id,
            'cron_expression': job.cron_expression,
            'enabled': job.enabled,
            'created_at': job.created_at.isoformat()
        })
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Failed to update job: {str(e)}'}), 500



@bp.route('/run_now/<int:script_id>', methods=['POST'])
def run_script_now(script_id):
    """Immediately run a script"""
    script = Script.query.get_or_404(script_id)

    try:
        # Create a temporary job for immediate execution
        temp_job = Job(
            job_id=f'temp_{script_id}_{datetime.utcnow().timestamp()}',
            script_id=script_id,
            cron_expression='once'
        )
        db.session.add(temp_job)
        db.session.commit()

        try:
            execute_script(temp_job.id)
            return jsonify({'message': 'Script execution started'}), 202
        except Exception as e:
            raise e
        finally:
            # Clean up temporary job
            db.session.delete(temp_job)
            db.session.commit()

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Failed to run script: {str(e)}'}), 500


@bp.route('/executions/<int:job_id>', methods=['GET'])
def get_job_executions(job_id):
    """Get execution history for a job"""
    executions = JobExecution.query.filter_by(job_id=job_id).order_by(JobExecution.start_time.desc()).all()
    return jsonify([{
        'id': exe.id,
        'start_time': exe.start_time.isoformat(),
        'end_time': exe.end_time.isoformat() if exe.end_time else None,
        'status': exe.status,
        'error_message': exe.error_message
    } for exe in executions])