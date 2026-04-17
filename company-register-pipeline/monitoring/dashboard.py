"""Simple Flask dashboard for pipeline monitoring."""

import os
import json
from datetime import datetime
from flask import Flask, render_template_string, jsonify, request
from monitoring.db_utils import MonitoringDB, DBConfig


app = Flask(__name__)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Monitor</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        .status-planned { background: #fee; }
        .status-running { background: #efe; }
        .status-completed { background: #cfc; }
        .status-failed { background: #fcc; }
        .status-stopped { background: #ccc; }
        .status-paused { background: #ffec8b; }
        table { border-collapse: collapse; width: 100%; background: white; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #4CAF50; color: white; }
        tr:hover { background: #f5f5f5; }
        .progress-bar { background: #4CAF50; height: 20px; border-radius: 4px; }
        .progress-bg { background: #ddd; height: 20px; border-radius: 4px; width: 100px; }
        .btn { padding: 5px 10px; margin: 2px; cursor: pointer; }
        .error { color: red; }
    </style>
</head>
<body>
    <h1>Pipeline Monitor</h1>
    <p>Last updated: {{ last_update }}</p>
    <table>
        <tr>
            <th>Pipeline</th>
            <th>Version</th>
            <th>Source</th>
            <th>Type</th>
            <th>Status</th>
            <th>Progress</th>
            <th>Items</th>
            <th>Server Load</th>
            <th>Started</th>
            <th>Duration</th>
            <th>Actions</th>
        </tr>
        {% for run in runs %}
        <tr class="status-{{ run.status }}">
            <td>{{ run.pipeline_name }}</td>
            <td>{{ run.pipeline_version }}</td>
            <td>{{ run.source_type }}</td>
            <td>{{ run.run_type }}</td>
            <td>{{ run.status }}</td>
            <td>
                <div class="progress-bg">
                    <div class="progress-bar" style="width: {{ run.progress_percent }}%"></div>
                </div>
                {{ run.progress_percent }}%
            </td>
            <td>{{ run.processed_items|default(0) }} / {{ run.total_items|default(0) }}</td>
            <td>{{ run.server_load_percent|default(0) }}%</td>
            <td>{{ run.started_at|default('-') }}</td>
            <td>{{ run.duration_minutes|default('-') }} min</td>
            <td>
                {% if run.status == 'running' %}
                <form method="POST" style="display:inline;">
                    <button type="submit" name="command" value="stop" class="btn">Stop</button>
                    <button type="submit" name="command" value="pause" class="btn">Pause</button>
                </form>
                {% elif run.status == 'paused' %}
                <form method="POST" style="display:inline;">
                    <button type="submit" name="command" value="resume" class="btn">Resume</button>
                    <button type="submit" name="command" value="stop" class="btn">Stop</button>
                </form>
                {% endif %}
            </td>
        </tr>
        {% endfor %}
    </table>
    <h2>Summary</h2>
    <ul>
        <li>Planned: {{ status_counts.planned|default(0) }}</li>
        <li>Running: {{ status_counts.running|default(0) }}</li>
        <li>Completed: {{ status_counts.completed|default(0) }}</li>
        <li>Failed: {{ status_counts.failed|default(0) }}</li>
        <li>Stopped: {{ status_counts.stopped|default(0) }}</li>
    </ul>
</body>
</html>
'''


@app.route('/')
def index():
    db = MonitoringDB()
    runs = db.get_all_runs_summary()
    db.close()
    
    status_counts = {}
    for run in runs:
        status = run.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    return render_template_string(
        HTML_TEMPLATE,
        runs=runs,
        status_counts=status_counts,
        last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )


@app.route('/api/runs')
def api_runs():
    db = MonitoringDB()
    runs = db.get_all_runs_summary()
    db.close()
    return jsonify(runs)


@app.route('/api/runs/<run_id>')
def api_run_detail(run_id):
    db = MonitoringDB()
    details = db.get_run_details(run_id)
    db.close()
    return jsonify(details)


@app.route('/api/command/<run_id>', methods=['POST'])
def api_command(run_id):
    command = request.form.get('command')
    if command not in ['pause', 'resume', 'stop']:
        return jsonify({'error': 'Invalid command'}), 400
    
    db = MonitoringDB()
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_run_commands (run_id, command)
                VALUES (%s, %s)
            """, (run_id, command))
            conn.commit()
    return jsonify({'success': True, 'command': command})


if __name__ == '__main__':
    port = int(os.getenv('MONITOR_PORT', '8050'))
    app.run(host='0.0.0.0', port=port, debug=False)