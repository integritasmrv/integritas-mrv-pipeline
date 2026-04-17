"""Monitoring database utilities for pipeline tracking."""

import os
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import RealDictCursor


@dataclass
class DBConfig:
    host: str = os.getenv('MONITOR_DB_HOST', '81.17.101.186')
    port: int = int(os.getenv('MONITOR_DB_PORT', '5434'))
    database: str = os.getenv('MONITOR_DB_NAME', 'kbo_v1')
    user: str = os.getenv('MONITOR_DB_USER', 'aiuser')
    password: str = os.getenv('MONITOR_DB_PASSWORD', 'aipassword123')


class MonitoringDB:
    """Database connection and operations for pipeline monitoring."""

    def __init__(self, config: Optional[DBConfig] = None):
        self.config = config or DBConfig()
        self._conn = None

    def connect(self):
        """Establish database connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                cursor_factory=RealDictCursor
            )
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_run(
        self,
        pipeline_name: str,
        pipeline_version: str,
        source_type: str,
        run_type: str = 'initial',
        total_items: int = 0,
        total_files: int = 0,
        batch_size: int = 50,
        source_config: Optional[Dict] = None
    ) -> str:
        """Create a new pipeline run and return its ID."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_runs 
                    (pipeline_name, pipeline_version, source_type, run_type,
                     total_items, total_files, batch_size, source_config)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING run_id::text
                """, (pipeline_name, pipeline_version, source_type, run_type,
                      total_items, total_files, batch_size,
                      json.dumps(source_config or {})))
                return cur.fetchone()['run_id']

    def start_run(self, run_id: str) -> None:
        """Mark a pipeline run as started."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs 
                    SET status = 'running', started_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s
                """, (run_id,))
                conn.commit()

    def update_progress(
        self,
        run_id: str,
        processed_items: Optional[int] = None,
        processed_batches: Optional[int] = None,
        processed_files: Optional[int] = None,
        server_load: Optional[float] = None
    ) -> None:
        """Update run progress. Only updates DB once per minute to reduce load."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs 
                    SET processed_items = COALESCE(%s, processed_items),
                        processed_batches = COALESCE(%s, processed_batches),
                        processed_files = COALESCE(%s, processed_files),
                        server_load_percent = COALESCE(%s, server_load_percent),
                        progress_percent = CASE 
                            WHEN total_items > 0 THEN ROUND((COALESCE(%s, processed_items)::DECIMAL / total_items) * 100, 2)
                            ELSE 0
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s
                """, (processed_items, processed_batches, processed_files,
                      server_load, processed_items, run_id))
                conn.commit()

    def update_status(
        self,
        run_id: str,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """Update run status (completed, failed, stopped, paused)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs 
                    SET status = %s,
                        error_message = %s,
                        finished_at = CASE 
                            WHEN %s IN ('completed', 'failed', 'stopped') 
                            THEN CURRENT_TIMESTAMP ELSE NULL END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s
                """, (status, error_message, status, run_id))
                conn.commit()

    def get_command(self, run_id: str) -> Optional[str]:
        """Get any pending command for a run (pause, resume, stop)."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT command FROM pipeline_run_commands 
                    WHERE run_id = %s AND executed = FALSE
                    ORDER BY issued_at DESC LIMIT 1
                """, (run_id,))
                result = cur.fetchone()
                if result:
                    cur.execute("""
                        UPDATE pipeline_run_commands 
                        SET executed = TRUE, executed_at = CURRENT_TIMESTAMP
                        WHERE run_id = %s AND executed = FALSE
                    """, (run_id,))
                    conn.commit()
                    return result['command']
                return None

    def add_item(
        self,
        run_id: str,
        item_type: str,
        source_path: str,
        table_name: str,
        total_items: int = 0
    ) -> str:
        """Add an item (file/API) to track within a run."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_run_items
                    (run_id, item_type, source_path, table_name, total_items)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING item_id::text
                """, (run_id, item_type, source_path, table_name, total_items))
                return cur.fetchone()['item_id']

    def start_item(self, item_id: str) -> None:
        """Mark an item as started."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_run_items 
                    SET status = 'running', started_at = CURRENT_TIMESTAMP
                    WHERE item_id = %s
                """, (item_id,))
                conn.commit()

    def update_item_progress(
        self,
        item_id: str,
        processed_items: Optional[int] = None,
        status: Optional[str] = None
    ) -> None:
        """Update item progress."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_run_items 
                    SET processed_items = COALESCE(%s, processed_items),
                        status = COALESCE(%s, status),
                        progress_percent = CASE 
                            WHEN total_items > 0 THEN ROUND((COALESCE(%s, processed_items)::DECIMAL / total_items) * 100, 2)
                            ELSE 0
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE item_id = %s
                """, (processed_items, status, processed_items, item_id))
                conn.commit()

    def complete_item(self, item_id: str, error: Optional[str] = None) -> None:
        """Mark an item as completed or failed."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_run_items 
                    SET status = CASE WHEN %s IS NULL THEN 'completed' ELSE 'failed' END,
                        error_message = %s,
                        finished_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE item_id = %s
                """, (error, error, item_id))
                conn.commit()

    def get_pipelines(self, status: Optional[str] = None) -> List[Dict]:
        """Get pipeline runs, optionally filtered by status."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                if status:
                    cur.execute("""
                        SELECT * FROM pipeline_runs 
                        WHERE status = %s
                        ORDER BY created_at DESC
                    """, (status,))
                else:
                    cur.execute("""
                        SELECT * FROM pipeline_runs 
                        ORDER BY created_at DESC
                    """)
                return cur.fetchall()

    def get_run_details(self, run_id: str) -> Dict:
        """Get full details of a run including its items."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM pipeline_runs WHERE run_id = %s", (run_id,))
                run = cur.fetchone()
                cur.execute("""
                    SELECT * FROM pipeline_run_items 
                    WHERE run_id = %s ORDER BY created_at
                """, (run_id,))
                items = cur.fetchall()
                return {'run': dict(run), 'items': [dict(i) for i in items]}

    def get_all_runs_summary(self) -> List[Dict]:
        """Get summary of all runs for dashboard."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        run_id,
                        pipeline_name,
                        pipeline_version,
                        source_type,
                        run_type,
                        status,
                        progress_percent,
                        total_items,
                        processed_items,
                        total_files,
                        processed_files,
                        server_load_percent,
                        batch_size,
                        started_at,
                        finished_at,
                        created_at,
                        updated_at,
                        EXTRACT(EPOCH FROM (updated_at - started_at))/60 as duration_minutes
                    FROM pipeline_runs 
                    ORDER BY created_at DESC
                    LIMIT 50
                """)
                return [dict(row) for row in cur.fetchall()]