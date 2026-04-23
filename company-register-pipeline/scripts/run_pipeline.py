#!/usr/bin/env python3
"""
Production Pipeline for Company Register Data
=============================================
Simple, fast COPY-based merge.
Strategy: COPY source -> staging -> DELETE matching -> INSERT FROM staging

Version DB schemas match CSV headers exactly (uppercase, all TEXT for flexibility).
Master DB schemas match production (created by 01_create_master_db.sql).
Merge maps version (uppercase) columns to master (lowercase) columns case-insensitively.
"""
import sys
import os
import logging
import csv
from datetime import datetime
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '5434'))
DB_USER = os.getenv('DB_USER', 'aiuser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'aipassword123')
MASTER_DB = 'BE KBO MASTER'

TABLES = [
    ("Enterprise.csv", "kbo_master.enterprise", "kbo.enterprise"),
    ("Establishment.csv", "kbo_master.establishment", "kbo.establishment"),
    ("Denomination.csv", "kbo_master.denomination", "kbo.denomination"),
    ("Address.csv", "kbo_master.address", "kbo.address"),
    ("Contact.csv", "kbo_master.contact", "kbo.contact"),
    ("Activity.csv", "kbo_master.activity", "kbo.activity"),
    ("Branch.csv", "kbo_master.branch", "kbo.branch"),
    ("Code.csv", "kbo_master.code", "kbo.code"),
]

PK_COLS = {
    'enterprise': ['enterprisenumber'],
    'establishment': ['establishmentnumber'],
    'denomination': ['entitynumber', 'language', 'typeofdenomination'],
    'address': ['entitynumber', 'typeofaddress'],
    'contact': ['entitynumber', 'entitycontact', 'contacttype'],
    'activity': ['entitynumber', 'activitygroup', 'naceversion', 'nacecode'],
    'branch': ['id'],
    'code': ['category', 'code', 'language'],
}


def get_conn(db):
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, database=db, user=DB_USER, password=DB_PASSWORD)


def init_master():
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
            enterprisenumber VARCHAR(20) PRIMARY KEY, status VARCHAR(2),
            juridicalsituation VARCHAR(10), typeofenterprise VARCHAR(3),
            juridicalform VARCHAR(10), juridicalformcac VARCHAR(10), startdate VARCHAR(20))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.establishment (
            establishmentnumber VARCHAR(20) PRIMARY KEY,
            startdate VARCHAR(20),
            enterprisenumber VARCHAR(20))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.denomination (
            entitynumber VARCHAR(20) NOT NULL,
            language VARCHAR(2) NOT NULL,
            typeofdenomination SMALLINT NOT NULL,
            denomination TEXT,
            PRIMARY KEY (entitynumber, language, typeofdenomination))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.address (
            entitynumber VARCHAR(20) NOT NULL,
            typeofaddress VARCHAR(20) NOT NULL,
            countrynl TEXT, countryfr TEXT,
            zipcode VARCHAR(20),
            municipalitynl TEXT, municipalityfr TEXT,
            streetnl TEXT, streetfr TEXT,
            housenumber TEXT, box TEXT, extraaddressinfo TEXT,
            datestrikingoff DATE,
            PRIMARY KEY (entitynumber, typeofaddress))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.contact (
            entitynumber VARCHAR(20) NOT NULL,
            entitycontact VARCHAR(3) NOT NULL,
            contacttype VARCHAR(10) NOT NULL,
            value TEXT,
            PRIMARY KEY (entitynumber, entitycontact, contacttype))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.activity (
            entitynumber VARCHAR(20), activitygroup VARCHAR(5), naceversion VARCHAR(10),
            nacecode VARCHAR(10), classification VARCHAR(5),
            PRIMARY KEY (entitynumber, activitygroup, naceversion, nacecode))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.branch (
            id VARCHAR(20) PRIMARY KEY,
            startdate VARCHAR(20),
            enterprisenumber VARCHAR(20))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS kbo_master.code (
            category TEXT NOT NULL,
            code TEXT NOT NULL,
            language VARCHAR(2) NOT NULL,
            description TEXT,
            PRIMARY KEY (category, code, language))""")
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
        for sql in [
            # Columns match CSV headers exactly, all TEXT for flexibility
            """CREATE TABLE kbo.enterprise (
                EnterpriseNumber TEXT PRIMARY KEY, Status TEXT,
                JuridicalSituation TEXT, TypeOfEnterprise TEXT,
                JuridicalForm TEXT, JuridicalFormCAC TEXT, StartDate TEXT)""",
            """CREATE TABLE kbo.establishment (
                EstablishmentNumber TEXT PRIMARY KEY,
                StartDate TEXT,
                EnterpriseNumber TEXT)""",
            """CREATE TABLE kbo.denomination (
                EntityNumber TEXT, Language TEXT,
                TypeOfDenomination TEXT, Denomination TEXT)""",
            """CREATE TABLE kbo.address (
                EntityNumber TEXT, TypeOfAddress TEXT,
                CountryNL TEXT, CountryFR TEXT, Zipcode TEXT,
                MunicipalityNL TEXT, MunicipalityFR TEXT,
                StreetNL TEXT, StreetFR TEXT,
                HouseNumber TEXT, Box TEXT, ExtraAddressInfo TEXT,
                DateStrikingOff TEXT)""",
            """CREATE TABLE kbo.contact (
                EntityNumber TEXT, EntityContact TEXT,
                ContactType TEXT, Value TEXT)""",
            """CREATE TABLE kbo.activity (
                EntityNumber TEXT, ActivityGroup TEXT,
                NaceVersion TEXT, NaceCode TEXT, Classification TEXT)""",
            """CREATE TABLE kbo.branch (
                Id TEXT PRIMARY KEY, StartDate TEXT,
                EnterpriseNumber TEXT)""",
            """CREATE TABLE kbo.code (
                Category TEXT, Code TEXT,
                Language TEXT, Description TEXT)""",
        ]:
            cur.execute(sql)
    conn.commit()
    conn.close()
    return dbname


def load_csv(conn, csv_path, table):
    with open(csv_path, 'r', encoding='latin-1', errors='replace') as f:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '')", f)
    conn.commit()


def load_extract(extract_path, label):
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
        for csv_file, master_table, version_table in TABLES:
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
    """Fast merge: COPY source -> staging -> DELETE+INSERT"""
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
    try:
        for csv_file, master_table, version_table in TABLES:
            table_name = master_table.split('.')[1]
            pkeys = PK_COLS.get(table_name, [])
            logger.info(f"Merging {master_table}...")
            with version_conn.cursor() as cur:
                schema, tbl = version_table.split('.')
                cur.execute("""SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""", (schema, tbl))
                src_cols = [r[0] for r in cur.fetchall()]
            with master_conn.cursor() as cur:
                cur.execute("""SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'kbo_master' AND table_name = %s ORDER BY ordinal_position""", (table_name,))
                master_cols = [r[0] for r in cur.fetchall()]
            master_cols_lower = {c.lower(): c for c in master_cols}
            src_to_master = {}
            for sc in src_cols:
                if sc.lower() in master_cols_lower:
                    src_to_master[sc] = master_cols_lower[sc.lower()]
            mapped_src = [c for c in src_cols if c in src_to_master]
            mapped_master = [src_to_master[c] for c in mapped_src]
            logger.info(f"  Mapped {len(mapped_src)}/{len(src_cols)} cols: {mapped_master}")
            with version_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {version_table}")
                source_count = cur.fetchone()[0]
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                master_before = cur.fetchone()[0]
            logger.info(f"  Source: {source_count:,}, Master before: {master_before:,}")
            temp_file = f'/tmp/{table_name}_merge.csv'
            with version_conn.cursor() as cur:
                cols_quoted = ['"' + c + '"' for c in mapped_src]
                cur.execute(f"SELECT {', '.join(cols_quoted)} FROM {version_table}")
                with open(temp_file, 'w', encoding='utf-8', errors='replace', newline='') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_ALL, delimiter=',')
                    writer.writerow(mapped_master)
                    while True:
                        rows = cur.fetchmany(50000)
                        if not rows:
                            break
                        writer.writerows(rows)
            logger.info(f"  Exported to {temp_file}")
            staging_table = f'kbo_merge_staging_{table_name}'
            with master_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                col_defs = [f'"{c}" TEXT' for c in mapped_master]
                cur.execute(f"CREATE TABLE {staging_table} ({', '.join(col_defs)})")
            master_conn.commit()
            with open(temp_file, 'r', encoding='utf-8', errors='replace') as f:
                with master_conn.cursor() as cur:
                    next(f)
                    cur.copy_expert(f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '')", f)
            master_conn.commit()
            os.remove(temp_file)
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
                staging_count = cur.fetchone()[0]
            logger.info(f"  Staged: {staging_count:,}")
            deleted = 0
            if pkeys:
                where_parts = [f'm."{pk}" = s."{pk}"' for pk in pkeys]
                where_clause = ' AND '.join(where_parts)
                with master_conn.cursor() as cur:
                    cur.execute(f"""DELETE FROM {master_table} m
                        WHERE EXISTS (SELECT 1 FROM {staging_table} s WHERE {where_clause})""")
                    deleted = cur.rowcount
                master_conn.commit()
                logger.info(f"  Deleted: {deleted:,}")
            col_list = ', '.join([f'"{c}"' for c in mapped_master])
            with master_conn.cursor() as cur:
                cur.execute(f"INSERT INTO {master_table} ({col_list}) SELECT {col_list} FROM {staging_table}")
                inserted = cur.rowcount
            master_conn.commit()
            logger.info(f"  Inserted: {inserted:,}")
            with master_conn.cursor() as cur:
                cur.execute(f"DROP TABLE {staging_table}")
            master_conn.commit()
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                master_after = cur.fetchone()[0]
            with master_conn.cursor() as cur:
                cur.execute("""INSERT INTO public.pipeline_metrics
                    (extract_version, table_name, operation, rows_count)
                    VALUES (%s, %s, 'MERGED', %s)""", (label, table_name, source_count))
            master_conn.commit()
            logger.info(f"  {master_table}: {source_count:,} merged ({deleted:,} replaced, {inserted:,} new) -> {master_after:,}")
            total_ops += source_count
        with master_conn.cursor() as cur:
            cur.execute("""UPDATE public.pipeline_state
                SET status = 'merged', merge_completed_at = NOW(), records_merged = %s
                WHERE extract_version = %s""", (total_ops, label))
            master_conn.commit()
        logger.info(f"Merge complete: {total_ops:,} total")
        master_conn.close()
        version_conn.close()
        return True
    except Exception as e:
        logger.error(f"Merge failed: {e}")
        import traceback
        traceback.print_exc()
        with master_conn.cursor() as cur:
            cur.execute("ROLLBACK")
        with master_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'failed', error_message = %s WHERE extract_version = %s", (str(e)[:500], label))
            master_conn.commit()
        master_conn.close()
        version_conn.close()
        raise


def show_status():
    conn = get_conn(MASTER_DB)
    print("\n" + "="*70)
    print("PIPELINE STATUS")
    print("="*70)
    with conn.cursor() as cur:
        cur.execute("""SELECT extract_version, status, records_loaded, records_merged, error_message
            FROM public.pipeline_state ORDER BY extract_version""")
        for row in cur.fetchall():
            print(f"\nVersion {row[0]}: {row[1]}")
            print(f"  Loaded: {row[2] or 0:,} records")
            print(f"  Merged: {row[3] or 0:,} records")
            if row[4]:
                print(f"  Error: {row[4]}")
    print("\n" + "-"*70)
    print("METRICS")
    print("-"*70)
    with conn.cursor() as cur:
        cur.execute("""SELECT extract_version, table_name, operation, SUM(rows_count)
            FROM public.pipeline_metrics
            GROUP BY extract_version, table_name, operation
            ORDER BY extract_version, table_name""")
        for row in cur.fetchall():
            print(f"  {row[0]}/{row[1]}/{row[2]}: {row[3]:,}")
    print("\n" + "-"*70)
    print("MASTER TABLE COUNTS")
    print("-"*70)
    with conn.cursor() as cur:
        cur.execute("""SELECT 'activity' as tbl, COUNT(*) FROM kbo_master.activity
            UNION ALL SELECT 'address', COUNT(*) FROM kbo_master.address
            UNION ALL SELECT 'branch', COUNT(*) FROM kbo_master.branch
            UNION ALL SELECT 'code', COUNT(*) FROM kbo_master.code
            UNION ALL SELECT 'contact', COUNT(*) FROM kbo_master.contact
            UNION ALL SELECT 'denomination', COUNT(*) FROM kbo_master.denomination
            UNION ALL SELECT 'enterprise', COUNT(*) FROM kbo_master.enterprise
            UNION ALL SELECT 'establishment', COUNT(*) FROM kbo_master.establishment
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
        init_master()
        if not merge_only:
            load_extract(extract_path, label)
        if not load_only:
            merge_extract(label)
        show_status()