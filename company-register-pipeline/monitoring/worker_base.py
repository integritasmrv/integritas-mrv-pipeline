"""Base worker class for batch processing with pause/stop/resume support."""

import os
import sys
import time
import signal
import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Generator
from datetime import datetime
from pathlib import Path

from monitoring.db_utils import MonitoringDB, DBConfig


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchWorker(ABC):
    """Base class for batch processing with monitoring and control.
    
    Usage:
        class MyLoader(BatchWorker):
            def get_items(self): ...
            def process_batch(self, batch): ...
        
        worker = MyLoader(run_id='...')
        worker.run()
    """

    def __init__(
        self,
        run_id: str,
        db_config: Optional[DBConfig] = None,
        batch_size: int = 50,
        progress_update_interval: int = 60
    ):
        self.run_id = run_id
        self.db = MonitoringDB(db_config)
        self.batch_size = batch_size
        self.progress_update_interval = progress_update_interval
        
        self.total_processed = 0
        self.total_batches = 0
        self.last_progress_update = None
        self._should_stop = False
        self._should_pause = False
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle SIGINT/SIGTERM gracefully."""
        logger.info(f"Received signal {signum}, will stop after current batch")
        self._should_stop = True

    def check_command(self) -> Optional[str]:
        """Check for pause/stop commands from database."""
        try:
            cmd = self.db.get_command(self.run_id)
            if cmd == 'stop':
                logger.info("Stop command received")
                self._should_stop = True
            elif cmd == 'pause':
                logger.info("Pause command received")
                self._should_pause = True
            return cmd
        except Exception as e:
            logger.warning(f"Failed to check command: {e}")
            return None

    def update_progress(self, force: bool = False) -> None:
        """Update progress in database.
        
        Args:
            force: Force update even if less than interval elapsed
        """
        now = datetime.now()
        if not force and self.last_progress_update:
            if (now - self.last_progress_update).seconds < self.progress_update_interval:
                return
        
        try:
            self.db.update_progress(
                self.run_id,
                processed_items=self.total_processed,
                processed_batches=self.total_batches,
                server_load=self.get_server_load()
            )
            self.last_progress_update = now
            logger.info(f"Progress: {self.total_processed} items, {self.total_batches} batches")
        except Exception as e:
            logger.warning(f"Failed to update progress: {e}")

    def get_server_load(self) -> Optional[float]:
        """Get current server load percentage."""
        try:
            if sys.platform == 'win32':
                import psutil
                return psutil.cpu_percent(interval=0.1)
            else:
                with open('/proc/loadavg') as f:
                    load = float(f.read().split()[0])
                return min(load * 10, 100)  
        except:
            return None

    @abstractmethod
    def get_items(self) -> Generator[Any, None, None]:
        """Yield items to process one by one.
        
        Yields:
            Each item to be processed
        """
        raise NotImplementedError

    @abstractmethod
    def process_batch(self, batch: List[Any]) -> int:
        """Process a batch of items.
        
        Args:
            batch: List of items to process
            
        Returns:
            Number of successfully processed items
        """
        raise NotImplementedError

    def run(self):
        """Main processing loop."""
        logger.info(f"Starting worker for run {self.run_id}")
        
        try:
            self.db.start_run(self.run_id)
            self.db.update_status(self.run_id, 'running')
            
            batch = []
            for item in self.get_items():
                if self._should_stop:
                    logger.info("Stop requested, exiting")
                    break
                
                while self._should_pause:
                    cmd = self.check_command()
                    if cmd == 'resume':
                        self._should_pause = False
                        logger.info("Resuming")
                        break
                    elif self._should_stop:
                        break
                    time.sleep(5)
                
                batch.append(item)
                if len(batch) >= self.batch_size:
                    self._process_batch(batch)
                    batch = []
                    self.check_command()
            
            if batch:
                self._process_batch(batch)
            
            if self._should_stop:
                self.db.update_status(self.run_id, 'stopped')
            else:
                self.db.update_status(self.run_id, 'completed')
                
        except Exception as e:
            logger.error(f"Worker failed: {e}")
            self.db.update_status(self.run_id, 'failed', str(e))
            raise
        finally:
            self.update_progress(force=True)
            self.db.close()

    def _process_batch(self, batch: List[Any]) -> None:
        """Process and update progress for a batch."""
        try:
            processed = self.process_batch(batch)
            self.total_processed += processed
            self.total_batches += 1
            self.update_progress()
        except Exception as e:
            logger.error(f"Batch failed: {e}")
            raise


class CSVBatchWorker(BatchWorker):
    """Worker for processing CSV files row by row."""

    def __init__(
        self,
        run_id: str,
        file_path: str,
        item_id: str,
        db_config: Optional[DBConfig] = None,
        batch_size: int = 50,
        encoding: str = 'latin-1'
    ):
        super().__init__(run_id, db_config, batch_size)
        self.file_path = file_path
        self.item_id = item_id
        self.encoding = encoding
        self.headers = []
        self._file = None

    def get_items(self) -> Generator[Dict, None, None]:
        """Yield rows from CSV file."""
        import csv
        
        self.headers = []
        with open(self.file_path, 'r', encoding=self.encoding, errors='replace') as f:
            reader = csv.DictReader(f)
            self.headers = reader.fieldnames or []
            for row in reader:
                yield row

    def process_batch(self, batch: List[Dict]) -> int:
        """Insert batch into database. Override in subclass."""
        raise NotImplementedError


def create_and_start_run(
    pipeline_name: str,
    pipeline_version: str,
    source_type: str,
    run_type: str = 'initial',
    total_items: int = 0,
    total_files: int = 0,
    batch_size: int = 50,
    files: Optional[List[Dict]] = None,
    db_config: Optional[DBConfig] = None
) -> str:
    """Create a new pipeline run and return its ID.
    
    Args:
        pipeline_name: Name of pipeline (e.g., 'kbo_initial_load')
        pipeline_version: Version string (e.g., '1.0.0')
        source_type: Source type (e.g., 'KBO', 'UK_CH')
        run_type: Type of run ('initial', 'merge', 'api')
        total_items: Total records to process
        total_files: Total files to process
        batch_size: Batch size for processing
        files: List of file dicts {'path': str, 'table': str, 'records': int}
        db_config: Database configuration
    
    Returns:
        run_id of the created run
    """
    db = MonitoringDB(db_config)
    
    run_id = db.create_run(
        pipeline_name=pipeline_name,
        pipeline_version=pipeline_version,
        source_type=source_type,
        run_type=run_type,
        total_items=total_items,
        total_files=total_files,
        batch_size=batch_size
    )
    
    if files:
        for f in files:
            db.add_item(
                run_id=run_id,
                item_type='file',
                source_path=f['path'],
                table_name=f.get('table', ''),
                total_items=f.get('records', 0)
            )
    
    db.start_run(run_id)
    db.close()
    
    logger.info(f"Created run {run_id} for {pipeline_name}")
    return run_id


def issue_command(run_id: str, command: str, db_config: Optional[DBConfig] = None) -> None:
    """Issue a command (pause, resume, stop) to a running pipeline.
    
    Args:
        run_id: ID of the run to control
        command: Command ('pause', 'resume', 'stop')
        db_config: Database configuration
    """
    db = MonitoringDB(db_config)
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_run_commands (run_id, command)
                VALUES (%s, %s)
            """, (run_id, command))
            conn.commit()
    logger.info(f"Issued {command} command to run {run_id}")