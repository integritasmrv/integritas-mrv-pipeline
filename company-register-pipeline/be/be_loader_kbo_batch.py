"""KBO batch loader with pause/stop/resume support."""

import os
import sys
import csv
import logging
from datetime import datetime
from typing import List, Dict, Generator

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monitoring.db_utils import MonitoringDB, DBConfig
from monitoring.worker_base import BatchWorker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


KBO_FILES = {
    'enterprise': {'table': 'kbo_enterprise', 'date_cols': ['StartDate']},
    'establishment': {'table': 'kbo_establishment', 'date_cols': ['StartDate']},
    'address': {'table': 'kbo_address', 'date_cols': ['DateStrikingOff']},
    'contact': {'table': 'kbo_contact', 'date_cols': []},
    'denomination': {'table': 'kbo_denomination', 'date_cols': []},
    'activity': {'table': 'kbo_activity', 'date_cols': []},
    'branch': {'table': 'kbo_branch', 'date_cols': ['StartDate']},
    'code': {'table': 'kbo_code', 'date_cols': []},
    'meta': {'table': 'kbo_meta', 'date_cols': []},
}


class KBOBatchLoader(BatchWorker):
    """Batch loader for KBO CSV files with row-by-row processing."""

    def __init__(
        self,
        run_id: str,
        data_dir: str,
        db_config: DBConfig = None,
        batch_size: int = 50
    ):
        super().__init__(run_id, db_config, batch_size)
        self.data_dir = data_dir
        self.current_file = None
        self.current_item_id = None
        self._file_count = 0
        self._total_files = len(KBO_FILES)

    def get_items(self) -> Generator[Dict, None, None]:
        """Process all KBO files sequentially."""
        for filename, config in KBO_FILES.items():
            filepath = os.path.join(self.data_dir, f"ExtractNumber 300", f"{filename}.csv")
            
            if not os.path.exists(filepath):
                logger.warning(f"File not found: {filepath}")
                continue
            
            logger.info(f"Processing file: {filename}")
            self.current_file = filename
            
            item_id = self.db.add_item(
                run_id=self.run_id,
                item_type='file',
                source_path=filepath,
                table_name=config['table'],
                total_items=self._count_lines(filepath)
            )
            self.current_item_id = item_id
            self.db.start_item(item_id)
            self._file_count += 1
            
            try:
                with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if self._should_stop:
                            break
                        row['_table'] = config['table']
                        row['_date_cols'] = config['date_cols']
                        yield row
            except Exception as e:
                logger.error(f"Error reading {filepath}: {e}")
                self.db.complete_item(item_id, str(e))

        self._should_stop = True

    def process_batch(self, batch: List[Dict]) -> int:
        """Insert batch into appropriate table."""
        if not batch:
            return 0
        
        table = batch[0].get('_table', 'kbo_enterprise')
        date_cols = batch[0].get('_date_cols', [])
        
        conn = psycopg2.connect(
            host=self.db.config.host,
            port=self.db.config.port,
            database=self.db.config.database,
            user=self.db.config.user,
            password=self.db.config.password
        )
        
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SET datestyle TO 'DMY'")
                    
                    cols = [c for c in batch[0].keys() if not c.startswith('_')]
                    col_list = ', '.join(cols)
                    placeholders = ', '.join(['%s'] * len(cols))
                    
                    insert_sql = f"""
                        INSERT INTO {table} ({col_list})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING
                    """
                    
                    for row in batch:
                        values = [row.get(c, '') or None for c in cols]
                        try:
                            cur.execute(insert_sql, values)
                        except Exception as e:
                            logger.debug(f"Insert error: {e}")
                    
                    conn.commit()
                    return len(batch)
        finally:
            conn.close()

    def _count_lines(self, filepath: str) -> int:
        """Count lines in file (excluding header)."""
        try:
            with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
                return sum(1 for _ in f) - 1
        except:
            return 0


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='KBO batch loader')
    parser.add_argument('--run-id', required=True, help='Pipeline run ID')
    parser.add_argument('--data-dir', required=True, help='Data directory')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size')
    parser.add_argument('--db-host', default='81.17.101.186')
    parser.add_argument('--db-port', type=int, default=5434)
    parser.add_argument('--db-name', default='kbo_v1')
    parser.add_argument('--db-user', default='aiuser')
    parser.add_argument('--db-password', default='aipassword123')
    
    args = parser.parse_args()
    
    db_config = DBConfig(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password
    )
    
    loader = KBOBatchLoader(
        run_id=args.run_id,
        data_dir=args.data_dir,
        db_config=db_config,
        batch_size=args.batch_size
    )
    
    logger.info(f"Starting KBO loader for run {args.run_id}")
    loader.run()
    logger.info("Loader finished")


if __name__ == '__main__':
    main()