"""Script to start a KBO pipeline run."""

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monitoring.db_utils import MonitoringDB, DBConfig
from monitoring.worker_base import create_and_start_run


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def count_total_records(data_dir: str) -> tuple:
    """Count total records and files in data directory."""
    import os
    
    data_dir = os.path.join(data_dir, "ExtractNumber 300")
    total = 0
    files_info = []
    
    if not os.path.exists(data_dir):
        return 0, 0, []
    
    for fname in os.listdir(data_dir):
        if fname.endswith('.csv'):
            fpath = os.path.join(data_dir, fname)
            with open(fpath, 'r', encoding='latin-1', errors='replace') as f:
                count = sum(1 for _ in f) - 1  # Exclude header
            
            table_name = f"kbo_{fname.replace('.csv', '')}"
            files_info.append({'path': fpath, 'table': table_name, 'records': count})
            total += count
    
    return total, len([f for f in files_info if f['records'] > 0]), files_info


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Start KBO pipeline run')
    parser.add_argument('--data-dir', default='/tmp/kbo_data', help='Data directory')
    parser.add_argument('--version', default='1.0.0', help='Pipeline version')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size')
    parser.add_argument('--db-host', default='81.17.101.186')
    parser.add_argument('--db-port', type=int, default=5434)
    parser.add_argument('--db-name', default='kbo_v1')
    parser.add_argument('--db-user', default='aiuser')
    parser.add_argument('--db-password', default='aipassword123')
    parser.add_argument('--no-start', action='store_true', help='Only create, do not start')
    
    args = parser.parse_args()
    
    total_items, total_files, files_info = count_total_records(args.data_dir)
    
    logger.info(f"Total records: {total_items}, Total files: {total_files}")
    
    db_config = DBConfig(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password
    )
    
    run_id = create_and_start_run(
        pipeline_name='kbo_initial_load',
        pipeline_version=args.version,
        source_type='KBO',
        run_type='initial',
        total_items=total_items,
        total_files=total_files,
        batch_size=args.batch_size,
        files=files_info,
        db_config=db_config
    )
    
    logger.info(f"Created run: {run_id}")
    logger.info(f"Total items: {total_items}, Total files: {total_files}")
    
    if not args.no_start:
        logger.info("Starting loader...")
        
        from be.be_loader_kbo_batch import main as loader_main
        sys.argv = [
            'be_loader_kbo_batch.py',
            '--run-id', run_id,
            '--data-dir', args.data_dir,
            '--batch-size', str(args.batch_size),
            '--db-host', args.db_host,
            '--db-port', str(args.db_port),
            '--db-name', args.db_name,
            '--db-user', args.db_user,
            '--db-password', args.db_password
        ]
        loader_main()
    
    print(f"\nRun ID: {run_id}")
    print(f"To control: docker exec ... python monitoring/dashboard.py --port 8050")
    print(f"To stop: curl -X POST http://localhost:8050/api/command/{run_id} -d 'command=stop'")


if __name__ == '__main__':
    main()