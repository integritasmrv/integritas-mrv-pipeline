#!/usr/bin/env python3
"""
Production Pipeline for Company Register Data
=============================================
Proper batching: smaller batches with individual commits for each batch.
Never skips records - every source record is processed.

Usage: python run_pipeline.py <command> [args]
"""
import sys
import os
import logging
from datetime import datetime
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '5434'))
DB_USER = os.getenv('DB_USER', 'aiuser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'aipassword123')
MASTER_DB = 'BE KBO MASTER'

# Batch size: smaller batches = faster commits, better progress tracking
BATCH_SIZE = 1000

TABLES = [
    ("Enterprise.csv", "kbo_master.enterprise", "kbo.enterprise", ["EnterpriseNumber"]),
    ("Establishment.csv", "kbo_master.establishment", "kbo.establishment", ["EstablishmentNumber"]),
    ("Denomination.csv", "kbo_master.denomination", "kbo.denomination", ["EntityNumber"]),
    ("Address.csv", "kbo_master.address", "kbo.address", ["EntityNumber", "TypeOfAddress"]),
    ("Contact.csv", "kbo_master.contact", "kbo.contact", ["EntityNumber"]),
    ("Activity.csv", "kbo_master.activity", "kbo.activity", ["EntityNumber", "ActivityGroup", "NaceVersion", "NaceCode"]),
    ("Branch.csv", "kbo_master.branch", "kbo.branch", ["Id"]),
    ("Code.csv", "kbo_master.code", "kbo.code", ["EntityNumber"]),
]


def get_conn(db):
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, database=db, user=DB_USER, password=DB_PASSWORD)


def init_master():
    """Create master database and schema if not exists."""
    logger.info("Initializing master database...")
    conn = get_conn("postgres")
    conn.set_isolation_level(0)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{MASTER_DB}"')
    except psycopg2.errors.DuplicateDatabase:
        pass
    conn.close()

    conn = get_conn(MASTER_DB)
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS kbo_master")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.enterprise (
            EnterpriseNumber VARCHAR(20) PRIMARY KEY, Status VARCHAR(2),
            JuridicalSituation VARCHAR(10), TypeOfEnterprise VARCHAR(3),
            JuridicalForm VARCHAR(10), JuridicalFormCAC VARCHAR(10), StartDate VARCHAR(20))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.establishment (
            EstablishmentNumber VARCHAR(20) PRIMARY KEY, EnterpriseNumber VARCHAR(20),
            StartDate VARCHAR(20), EntityNumber VARCHAR(20))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.denomination (
            EntityNumber VARCHAR(20) PRIMARY KEY, Denomination VARCHAR(500), Type VARCHAR(10), Language VARCHAR(3))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.address (
            EntityNumber VARCHAR(20), TypeOfAddress VARCHAR(20), Country VARCHAR(5),
            ZipCode VARCHAR(20), Municipality VARCHAR(100), Street VARCHAR(500),
            HouseNumber VARCHAR(20), Box VARCHAR(20), ExtraAddressInfo VARCHAR(500),
            PRIMARY KEY (EntityNumber, TypeOfAddress))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.contact (
            EntityNumber VARCHAR(20) PRIMARY KEY, Type VARCHAR(20), Value VARCHAR(500), Area VARCHAR(20), Language VARCHAR(3))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.activity (
            EntityNumber VARCHAR(20), ActivityGroup VARCHAR(20), NaceVersion VARCHAR(10),
            NaceCode VARCHAR(20), Classification VARCHAR(20),
            PRIMARY KEY (EntityNumber, ActivityGroup, NaceVersion, NaceCode))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.branch (
            Id SERIAL PRIMARY KEY, EnterpriseNumber VARCHAR(20), EstablishmentNumber VARCHAR(20), StartDate VARCHAR(20))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.code (
            EntityNumber VARCHAR(20) PRIMARY KEY, Type VARCHAR(50), Code VARCHAR(50))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS public.pipeline_state (
            id SERIAL PRIMARY KEY, extract_version VARCHAR(50) UNIQUE NOT NULL,
            status VARCHAR(20) DEFAULT 'pending', load_started_at TIMESTAMP,
            load_completed_at TIMESTAMP, merge_started_at TIMESTAMP,
            merge_completed_at TIMESTAMP, records_loaded BIGINT, records_merged BIGINT,
            error_message TEXT, created_at TIMESTAMP DEFAULT NOW())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS public.pipeline_metrics (
            id SERIAL PRIMARY KEY, extract_version VARCHAR(50) NOT NULL,
            table_name VARCHAR(50) NOT NULL, operation VARCHAR(20) NOT NULL,
            rows_count BIGINT NOT NULL, recorded_at TIMESTAMP DEFAULT NOW())""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_state_version ON public.pipeline_state(extract_version)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_version ON public.pipeline_metrics(extract_version, table_name)")
    conn.commit()
    conn.close()
    logger.info("Master database initialized.")


def get_version_db(label):
    return f"BE KBO {label}"


def create_version_db(label):
    """Create versioned database for an extract."""
    dbname = get_version_db(label)
    logger.info(f"Creating versioned database: {dbname}")
    conn = get_conn("postgres")
    conn.set_isolation_level(0)
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
            cur.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        conn.close()

    conn = get_conn(dbname)
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS kbo")
        cur.execute("""CREATE TABLE kbo.enterprise (
            EnterpriseNumber VARCHAR(20) PRIMARY KEY, Status VARCHAR(2),
            JuridicalSituation VARCHAR(10), TypeOfEnterprise VARCHAR(3),
            JuridicalForm VARCHAR(10), JuridicalFormCAC VARCHAR(10), StartDate VARCHAR(20))""")
        cur.execute("""CREATE TABLE kbo.establishment (
            EstablishmentNumber VARCHAR(20) PRIMARY KEY, EnterpriseNumber VARCHAR(20),
            StartDate VARCHAR(20), EntityNumber VARCHAR(20))""")
        cur.execute("""CREATE TABLE kbo.denomination (
            EntityNumber VARCHAR(20) PRIMARY KEY, Denomination VARCHAR(500), Type VARCHAR(10), Language VARCHAR(3))""")
        cur.execute("""CREATE TABLE kbo.address (
            EntityNumber VARCHAR(20), TypeOfAddress VARCHAR(20), Country VARCHAR(5),
            ZipCode VARCHAR(20), Municipality VARCHAR(100), Street VARCHAR(500),
            HouseNumber VARCHAR(20), Box VARCHAR(20), ExtraAddressInfo VARCHAR(500),
            PRIMARY KEY (EntityNumber, TypeOfAddress))""")
        cur.execute("""CREATE TABLE kbo.contact (
            EntityNumber VARCHAR(20) PRIMARY KEY, Type VARCHAR(20), Value VARCHAR(500), Area VARCHAR(20), Language VARCHAR(3))""")
        cur.execute("""CREATE TABLE kbo.activity (
            EntityNumber VARCHAR(20), ActivityGroup VARCHAR(20), NaceVersion VARCHAR(10),
            NaceCode VARCHAR(20), Classification VARCHAR(20),
            PRIMARY KEY (EntityNumber, ActivityGroup, NaceVersion, NaceCode))""")
        cur.execute("""CREATE TABLE kbo.branch (
            Id SERIAL PRIMARY KEY, EnterpriseNumber VARCHAR(20), EstablishmentNumber VARCHAR(20), StartDate VARCHAR(20))""")
        cur.execute("""CREATE TABLE kbo.code (
            EntityNumber VARCHAR(20) PRIMARY KEY, Type VARCHAR(50), Code VARCHAR(50))""")
    conn.commit()
    conn.close()
    return dbname


def load_csv(conn, csv_path, table):
    """Load CSV into table using COPY - fast bulk load."""
    with open(csv_path, 'r', encoding='latin-1', errors='replace') as f:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER E'\\t', NULL '')", f)
    conn.commit()


def load_extract(extract_path, label):
    """Load all CSVs from extract into versioned database."""
    dbname = get_version_db(label)
    state_conn = get_conn(MASTER_DB)

    with state_conn.cursor() as cur:
        cur.execute("SELECT status FROM public.pipeline_state WHERE extract_version = %s", (label,))
        row = cur.fetchone()
        if row and row[0] in ('loaded', 'merging', 'merged'):
            logger.info(f"Version {label} already {row[0]}, skipping load")
            return False

    with state_conn.cursor() as cur:
        cur.execute("""INSERT INTO public.pipeline_state (extract_version, status, load_started_at)
            VALUES (%s, 'loading', NOW())
            ON CONFLICT (extract_version) DO UPDATE SET status = 'loading', load_started_at = NOW()""", (label,))
        state_conn.commit()

    try:
        create_version_db(label)
        total = 0
        for csv_file, master_table, version_table, pkeys in TABLES:
            csv_path = os.path.join(extract_path, csv_file)
            if not os.path.exists(csv_path):
                logger.warning(f"CSV not found: {csv_path}, skipping")
                continue
            logger.info(f"Loading {csv_file}...")
            conn = get_conn(dbname)
            load_csv(conn, csv_path, version_table)
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {version_table}")
                cnt = cur.fetchone()[0]
            conn.close()
            logger.info(f"  -> {cnt:,} rows")
            total += cnt

        with state_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'loaded', load_completed_at = NOW(), records_loaded = %s WHERE extract_version = %s", (total, label))
            state_conn.commit()
        state_conn.close()
        logger.info(f"Load complete: {total:,} total records")
        return True
    except Exception as e:
        with state_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'failed', error_message = %s WHERE extract_version = %s", (str(e), label))
            state_conn.commit()
        state_conn.close()
        raise


def merge_extract(label):
    """Merge versioned database to master with proper batching.
    
    Uses smaller batches (1000 rows) with individual commits.
    Tracks INSERT vs UPDATE by counting before/after for each batch.
    """
    dbname = get_version_db(label)
    master_conn = get_conn(MASTER_DB)
    version_conn = get_conn(dbname)

    with master_conn.cursor() as cur:
        cur.execute("SELECT status FROM public.pipeline_state WHERE extract_version = %s", (label,))
        row = cur.fetchone()
        if row and row[0] == 'merged':
            logger.info(f"Version {label} already merged, skipping")
            return False

    with master_conn.cursor() as cur:
        cur.execute("UPDATE public.pipeline_state SET status = 'merging', merge_started_at = NOW() WHERE extract_version = %s", (label,))
        master_conn.commit()

    total_ops = 0
    total_inserts = 0
    total_updates = 0
    
    try:
        for csv_file, master_table, version_table, pkeys in TABLES:
            logger.info(f"Merging {master_table}...")
            
            # Get columns from version table
            with version_conn.cursor() as cur:
                schema, tbl = version_table.split('.')
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, tbl))
                cols = [r[0] for r in cur.fetchall()]

            if not cols:
                logger.warning(f"No columns found for {version_table}")
                continue

            # Build merge SQL
            pk_str = ", ".join(pkeys)
            non_pk = [c for c in cols if c not in pkeys]
            update_set = ", ".join([f"{c}=EXCLUDED.{c}" for c in non_pk]) if non_pk else "nothing=NOTHING"
            col_list = ", ".join(cols)
            placeholders = ", ".join(["%s"] * len(cols))
            
            merge_sql = f"""INSERT INTO {master_table} ({col_list}) 
                            VALUES ({placeholders}) 
                            ON CONFLICT ({pk_str}) DO UPDATE SET {update_set}"""

            # Get table row counts before merge
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                master_before = cur.fetchone()[0]

            with version_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {version_table}")
                version_total = cur.fetchone()[0]

            logger.info(f"  Source: {version_total:,} rows, Master before: {master_before:,}")

            # Process in smaller batches with individual commits
            version_cur = version_conn.cursor()
            version_cur.execute(f"SELECT {col_list} FROM {version_table} ORDER BY {pk_str}")

            batch_num = 0
            batch_inserts = 0
            batch_updates = 0
            offset = 0
            
            while True:
                batch = version_cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break

                # Get master count before this batch
                with master_conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                    before_batch = cur.fetchone()[0]

                # Execute batch with its own transaction
                master_cur = master_conn.cursor()
                master_cur.executemany(merge_sql, batch)
                master_conn.commit()

                # Get master count after this batch
                with master_conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                    after_batch = cur.fetchone()[0]

                # Calculate inserts vs updates for this batch
                net_new = after_batch - before_batch
                batch_insert = max(0, net_new)
                batch_update = len(batch) - batch_insert
                
                batch_inserts += batch_insert
                batch_updates += batch_update
                offset += len(batch)
                batch_num += 1

                # Progress every 100 batches
                if batch_num % 100 == 0:
                    logger.info(f"  {offset:,}/{version_total:,} ({100*offset/version_total:.1f}%) - +{batch_insert:,} ins, +{batch_update:,} upd")

            # Get final master count
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                master_after = cur.fetchone()[0]

            table_name = master_table.split('.')[1]
            
            # Record metrics for this table
            with master_conn.cursor() as cur:
                if batch_inserts > 0:
                    cur.execute("""INSERT INTO public.pipeline_metrics 
                        (extract_version, table_name, operation, rows_count) 
                        VALUES (%s, %s, 'INSERT', %s)""", (label, table_name, batch_inserts))
                if batch_updates > 0:
                    cur.execute("""INSERT INTO public.pipeline_metrics 
                        (extract_version, table_name, operation, rows_count) 
                        VALUES (%s, %s, 'UPDATE', %s)""", (label, table_name, batch_updates))
            master_conn.commit()

            logger.info(f"  {master_table}: {batch_inserts:,} INSERT, {batch_updates:,} UPDATE (total: {offset:,})")
            total_inserts += batch_inserts
            total_updates += batch_updates
            total_ops += offset

        # Mark as merged
        with master_conn.cursor() as cur:
            cur.execute("""UPDATE public.pipeline_state 
                SET status = 'merged', merge_completed_at = NOW(), records_merged = %s 
                WHERE extract_version = %s""", (total_ops, label))
            master_conn.commit()

        logger.info(f"Merge complete: {total_ops:,} total operations ({total_inserts:,} INSERT, {total_updates:,} UPDATE)")
        master_conn.close()
        version_conn.close()
        return True

    except Exception as e:
        logger.error(f"Merge failed: {e}")
        with master_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'failed', error_message = %s WHERE extract_version = %s", (str(e), label))
            master_conn.commit()
        master_conn.close()
        version_conn.close()
        raise


def show_status():
    """Display current pipeline status and metrics."""
    conn = get_conn(MASTER_DB)
    print("\n" + "="*70)
    print("PIPELINE STATUS")
    print("="*70)

    with conn.cursor() as cur:
        cur.execute("""SELECT extract_version, status, records_loaded, records_merged, error_message, 
            load_started_at, load_completed_at, merge_started_at, merge_completed_at 
            FROM public.pipeline_state ORDER BY extract_version""")
        for row in cur.fetchall():
            print(f"\nVersion {row[0]}: {row[1]}")
            print(f"  Loaded: {row[2] or 0:,} records")
            print(f"  Merged: {row[3] or 0:,} records")
            if row[4]:
                print(f"  Error: {row[4]}")
            if row[5]:
                print(f"  Load started: {row[5]}")
            if row[6]:
                print(f"  Load completed: {row[6]}")
            if row[7]:
                print(f"  Merge started: {row[7]}")
            if row[8]:
                print(f"  Merge completed: {row[8]}")

    print("\n" + "-"*70)
    print("METRICS (INSERT vs UPDATE)")
    print("-"*70)
    with conn.cursor() as cur:
        cur.execute("""SELECT extract_version, table_name, operation, SUM(rows_count) 
            FROM public.pipeline_metrics 
            GROUP BY extract_version, table_name, operation 
            ORDER BY extract_version, table_name, operation""")
        for row in cur.fetchall():
            print(f"  {row[0]}/{row[1]}/{row[2]}: {row[3]:,}")

    print("\n" + "-"*70)
    print("MASTER TABLE COUNTS")
    print("-"*70)
    with conn.cursor() as cur:
        cur.execute("""SELECT 'enterprise' as tbl, COUNT(*) FROM kbo_master.enterprise
            UNION ALL SELECT 'establishment', COUNT(*) FROM kbo_master.establishment
            UNION ALL SELECT 'address', COUNT(*) FROM kbo_master.address
            UNION ALL SELECT 'contact', COUNT(*) FROM kbo_master.contact
            UNION ALL SELECT 'activity', COUNT(*) FROM kbo_master.activity
            UNION ALL SELECT 'denomination', COUNT(*) FROM kbo_master.denomination
            UNION ALL SELECT 'branch', COUNT(*) FROM kbo_master.branch
            UNION ALL SELECT 'code', COUNT(*) FROM kbo_master.code
            ORDER BY tbl""")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,}")
    conn.close()
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "status":
        show_status()
    else:
        extract_path = command
        label = sys.argv[2]
        load_only = '--load-only' in sys.argv
        merge_only = '--merge-only' in sys.argv

        logger.info(f"Pipeline for version {label}")
        logger.info(f"Extract path: {extract_path}")
        logger.info(f"Batch size: {BATCH_SIZE}")

        init_master()

        if not merge_only:
            load_extract(extract_path, label)

        if not load_only:
            merge_extract(label)

        show_status()
