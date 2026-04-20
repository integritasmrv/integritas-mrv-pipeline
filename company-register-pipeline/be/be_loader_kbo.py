"""
BE KBO Pipeline Orchestrator - Chunked batch loader

Loads BE KBO data in chunks of 100 records per batch for reliability.
Sequential processing: one extract at a time, merge immediately after load.
"""

import os
import sys
import csv
import logging
import argparse
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db_utils import Database, DBConfig
from common import config_be as config
from monitoring.db_utils import MonitoringDB, DBConfig as MonitorDBConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def map_column_name(csv_name: str) -> str:
    """Map CSV column name to database column name (lowercase)."""
    return csv_name.strip().strip('"').lower()


class KBOPipeline:
    """BE KBO Pipeline Orchestrator."""

    def __init__(self, db_config: DBConfig = None, monitor_config: MonitorDBConfig = None):
        self.db = Database(db_config)
        self.monitor = MonitoringDB(monitor_config or MonitorDBConfig(database='kbo_v1'))
        self.run_id = None
        self.load_results = {}

    def run(self):
        """Execute the complete pipeline workflow."""
        logger.info("=" * 60)
        logger.info("BE KBO Pipeline Orchestrator starting")
        logger.info("=" * 60)

        self.run_id = None

        self._ensure_master_exists()

        new_extracts = self._find_unprocessed_extracts()

        if not new_extracts:
            logger.info("No new extracts found. All done.")
            return

        logger.info(f"Found {len(new_extracts)} new extract(s) to process")

        if new_extracts:
            try:
                self._process_extract(new_extracts[0])
            except Exception as e:
                logger.error(f"Failed to process {new_extracts[0]}: {e}")
                import traceback
                traceback.print_exc()

        logger.info("=" * 60)
        logger.info("Pipeline execution completed")
        logger.info("=" * 60)

    def _ensure_master_exists(self):
        """Ensure BE KBO MASTER exists. Create if it doesn't."""
        logger.info("Checking if BE KBO MASTER exists...")
        
        if self.db.database_exists(config.MASTER_DB):
            logger.info(f"BE KBO MASTER already exists")
        else:
            logger.info(f"Creating BE KBO MASTER...")
            self.db.create_database(config.MASTER_DB)
            
            master_db = Database(DBConfig(
                host=self.db.config.host,
                port=self.db.config.port,
                database=config.MASTER_DB,
                user=self.db.config.user,
                password=self.db.config.password
            ))
            master_db.run_sql_file(
                os.path.join(os.path.dirname(__file__), '..', 'sql', '01_create_be_master_db.sql')
            )
            logger.info(f"BE KBO MASTER created successfully")

    def _find_unprocessed_extracts(self) -> List[Path]:
        """Find extracts that haven't been merged yet."""
        if not os.path.exists(config.DATA_ROOT):
            logger.warning(f"Data root not found: {config.DATA_ROOT}")
            return []

        extract_folders = []
        for item in os.listdir(config.DATA_ROOT):
            if item.startswith(config.EXTRACT_PREFIX):
                extract_path = os.path.join(config.DATA_ROOT, item)
                if os.path.isdir(extract_path):
                    extract_folders.append(extract_path)

        extract_folders.sort(key=lambda x: self._extract_number_from_folder(x))

        master_db = Database(DBConfig(
            host=self.db.config.host,
            port=self.db.config.port,
            database=config.MASTER_DB,
            user=self.db.config.user,
            password=self.db.config.password
        ))
        
        unprocessed = []
        for folder in extract_folders:
            extract_num = self._extract_number_from_folder(folder)
            result = master_db.fetch_one(
                "SELECT 1 FROM ref.extract_version WHERE version_id = %s AND merge_status = 'DONE'",
                (str(extract_num),)
            )
            if not result:
                unprocessed.append(folder)

        return unprocessed

    def _extract_number_from_folder(self, folder_path: str) -> int:
        """Extract the sequence number from folder name."""
        folder_name = os.path.basename(folder_path)
        try:
            return int(folder_name.replace('ExtractNumber ', ''))
        except:
            return 0

    def _read_meta(self, extract_folder: str) -> Dict:
        """Read meta.csv to get extract metadata."""
        meta_path = os.path.join(extract_folder, 'meta.csv')
        meta = {}
        
        if os.path.exists(meta_path):
            with open(meta_path, 'r', encoding=config.CSV_ENCODING, errors='replace') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        meta[row[0].strip()] = row[1].strip()
        return meta

    def _process_extract(self, extract_folder: str):
        """Process a single extract: load CSVs then merge to master."""
        folder_name = os.path.basename(extract_folder)
        extract_num = self._extract_number_from_folder(extract_folder)
        versioned_db_name = f"BE KBO {extract_num}"
        
        logger.info("-" * 40)
        logger.info(f"Processing {folder_name}")
        logger.info("-" * 40)

        meta = self._read_meta(extract_folder)
        snapshot_date = meta.get('ExtractDate', datetime.now().strftime('%Y-%m-%d'))
        logger.info(f"Extract number: {extract_num}, Snapshot date: {snapshot_date}")

        extract_run_id = self.monitor.create_run(
            pipeline_name='be_kbo_load',
            pipeline_version='1.0',
            source_type='KBO',
            run_type='initial',
            total_files=len(config.KBO_FILES)
        )
        self.monitor.start_run(extract_run_id)
        self.current_run_id = extract_run_id

        if self.db.database_exists(versioned_db_name):
            logger.info(f"Dropping existing database: {versioned_db_name}")
            self.db.drop_database(versioned_db_name)
        
        logger.info(f"Creating versioned database: {versioned_db_name}")
        self.db.create_database(versioned_db_name)
        
        versioned_db = Database(DBConfig(
            host=self.db.config.host,
            port=self.db.config.port,
            database=versioned_db_name,
            user=self.db.config.user,
            password=self.db.config.password
        ))
        versioned_db.run_sql_file(
            os.path.join(os.path.dirname(__file__), '..', 'sql', '02_create_versioned_db.sql')
        )

        self.load_results = {}
        all_file_totals = {}
        grand_total = 0
        
        for csv_file in config.KBO_FILES:
            csv_path = os.path.join(extract_folder, csv_file)
            if os.path.exists(csv_path):
                total_rows = self._count_csv_rows(csv_path)
                all_file_totals[csv_file] = total_rows
                grand_total += total_rows
            else:
                all_file_totals[csv_file] = 0
        
        self.monitor.update_run_total_items(self.current_run_id, grand_total)
        
        item_ids = {}
        for csv_file in config.KBO_FILES:
            csv_path = os.path.join(extract_folder, csv_file)
            if os.path.exists(csv_path):
                item_id = self.monitor.add_item(
                    run_id=self.current_run_id,
                    item_type='file',
                    source_path=csv_path,
                    table_name=config.CSV_TO_TABLE.get(csv_file, f"kbo.{csv_file.replace('.csv', '')}"),
                    total_items=all_file_totals[csv_file]
                )
                item_ids[csv_file] = item_id
        
        processed_files = 0
        processed_items = 0
        
        for csv_file in config.KBO_FILES:
            csv_path = os.path.join(extract_folder, csv_file)
            item_id = item_ids.get(csv_file)
            
            if os.path.exists(csv_path) and item_id:
                self.monitor.start_item(item_id)
                self.current_item_id = item_id
                
                rows_loaded = self._load_csv_chunked(csv_path, csv_file, versioned_db, item_id)
                processed_items += rows_loaded
                
                if rows_loaded == 0 and all_file_totals[csv_file] > 0:
                    logger.error(f"    FAILED to load any rows from {csv_file} (expected {all_file_totals[csv_file]} rows)")
                    self.monitor.complete_item(item_id, "failed")
                    raise RuntimeError(f"Failed to load {csv_file}: inserted 0 rows from {all_file_totals[csv_file]} expected")
                
                self.load_results[csv_file] = rows_loaded
                processed_files += 1
                self.monitor.complete_item(item_id)
                self.monitor.update_progress(self.current_run_id, processed_items=processed_items, processed_files=processed_files)
            else:
                logger.warning(f"CSV file not found: {csv_path}")
                self.load_results[csv_file] = 0

        logger.info("Merging to BE KBO MASTER...")
        self._merge_to_master(versioned_db, str(extract_num), snapshot_date, extract_folder)

        logger.info(f"Completed processing {folder_name}")

    def _count_csv_rows(self, csv_path: str) -> int:
        """Count rows in CSV file (excluding header)."""
        try:
            with open(csv_path, 'r', encoding=config.CSV_ENCODING, errors='replace') as f:
                return sum(1 for _ in f) - 1
        except:
            return 0

    def _parse_date(self, value: str, date_format: str) -> str:
        """Convert date from various formats to PostgreSQL YYYY-MM-DD format."""
        if not value or value.strip() == '':
            return None
        value = value.strip()
        try:
            from datetime import datetime
            if date_format == 'DD-MM-YYYY':
                dt = datetime.strptime(value, '%d-%m-%Y')
                return dt.strftime('%Y-%m-%d')
            elif date_format == 'DD/MM/YYYY':
                dt = datetime.strptime(value, '%d/%m/%Y')
                return dt.strftime('%Y-%m-%d')
            elif date_format == 'YYYYMMDD':
                return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
            elif date_format == 'YYYY-MM-DD':
                return value
            else:
                return value
        except ValueError:
            return value

    def _transform_row(self, row: list, csv_headers: list, table_name: str) -> tuple:
        """Transform row values, converting dates as needed."""
        date_formats = config.DATE_COLUMNS.get(table_name, {})
        values = []
        for i, val in enumerate(row[:len(csv_headers)]):
            if i < len(csv_headers):
                col_name = csv_headers[i]
                if col_name in date_formats and val and val.strip():
                    val = self._parse_date(val, date_formats[col_name])
            if val is None or val.strip() == '':
                values.append(None)
            else:
                values.append(val.strip())
        return tuple(values)

    def _load_csv_chunked(self, csv_path: str, csv_file: str, db: Database, item_id: str = None) -> int:
        """Load CSV file in chunks of 200 records using batch INSERT."""
        table_name = config.CSV_TO_TABLE.get(csv_file, f"kbo.{csv_file.replace('.csv', '')}")
        logger.info(f"  Loading {csv_file} -> {table_name}")
        
        date_formats = config.DATE_COLUMNS.get(table_name, {})
        if date_formats:
            logger.info(f"    Date transformations enabled: {date_formats}")
        
        try:
            with open(csv_path, 'r', encoding=config.CSV_ENCODING, errors='replace') as f:
                csv_reader = csv.reader(f)
                csv_headers = [h.strip().strip('"') for h in next(csv_reader)]
            
            num_cols = len(csv_headers)
            col_list = ', '.join(csv_headers)
            placeholders = ', '.join(['%s'] * num_cols)
            insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            
            total_rows = 0
            total_read = 0
            batch = []
            
            with open(csv_path, 'r', encoding=config.CSV_ENCODING, errors='replace') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)
                
                for row in csv_reader:
                    total_read += 1
                    if len(row) >= num_cols:
                        values = self._transform_row(row[:num_cols], csv_headers, table_name)
                    else:
                        values = self._transform_row(row, csv_headers, table_name)
                    batch.append(values)
                    
                    if len(batch) >= 200:
                        try:
                            with db.connect() as conn:
                                with conn.cursor() as cur:
                                    cur.executemany(insert_sql, batch)
                                conn.commit()
                            total_rows += len(batch)
                            if item_id and total_rows % 1000 == 0:
                                self.monitor.update_item_progress(item_id, processed_items=total_rows)
                            if total_rows % 10000 == 0:
                                logger.info(f"    Loaded {total_rows:,} rows ({total_read:,} read)...")
                        except Exception as e:
                            logger.error(f"    Batch insert failed: {e}")
                            raise RuntimeError(f"Failed to insert batch at row {total_read}: {e}")
                        batch = []
                
                if batch:
                    try:
                        with db.connect() as conn:
                            with conn.cursor() as cur:
                                cur.executemany(insert_sql, batch)
                            conn.commit()
                        total_rows += len(batch)
                    except Exception as e:
                        logger.error(f"    Final batch insert failed: {e}")
                        raise RuntimeError(f"Failed to insert final batch: {e}")
            
            logger.info(f"    Loaded {total_rows:,} rows ({total_read:,} read)")
            return total_rows
            
        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"    Failed to load {csv_file}: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError(f"Failed to load {csv_file}: {e}")

    def _merge_to_master(self, versioned_db: Database, extract_num: str, 
                        snapshot_date: str, source_folder: str):
        """Merge versioned database to master using Python-based incremental merge."""
        logger.info("Merging to BE KBO MASTER...")
        
        sys.path.insert(0, os.path.dirname(__file__))
        from merge_be_to_master import KBOMergeToMaster
        
        merger_config = DBConfig(
            host=self.db.config.host,
            port=self.db.config.port,
            database='postgres',
            user=self.db.config.user,
            password=self.db.config.password
        )
        
        try:
            merger = KBOMergeToMaster(merger_config)
            result = merger.merge(
                version_id=extract_num,
                snapshot_date=snapshot_date,
                versioned_db_name=versioned_db.config.database,
                source_folder=source_folder
            )
            
            if result:
                logger.info(f"  Merge: {result.get('records_inserted', 0):,} ins, "
                           f"{result.get('records_updated', 0):,} upd, "
                           f"{result.get('records_queued', 0):,} queued")
            else:
                logger.info("  Merge completed")
        except Exception as e:
            logger.error(f"  Merge failed: {e}")
            import traceback
            traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description='BE KBO Pipeline Orchestrator')
    parser.add_argument('--db-host', default='127.0.0.1', help='Database host')
    parser.add_argument('--db-port', type=int, default=5434, help='Database port')
    parser.add_argument('--db-user', default='aiuser', help='Database user')
    parser.add_argument('--db-password', default='aipassword123', help='Database password')
    parser.add_argument('--data-root', default=config.DATA_ROOT, help='Data root directory')
    
    args = parser.parse_args()

    db_config = DBConfig(
        host=args.db_host,
        port=args.db_port,
        database='postgres',
        user=args.db_user,
        password=args.db_password
    )

    monitor_config = MonitorDBConfig(
        host=args.db_host,
        port=args.db_port,
        database='kbo_v1',
        user=args.db_user,
        password=args.db_password
    )

    pipeline = KBOPipeline(db_config, monitor_config)
    pipeline.run()


if __name__ == '__main__':
    main()