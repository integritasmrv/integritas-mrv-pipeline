#!/usr/bin/env python3
"""
Production Pipeline for Company Register Data
=============================================
Phase 1 (initial load): stream version DB -> staging -> INSERT ON CONFLICT DO UPDATE
Phase 2 (incremental):  reladiff to identify changed rows only, then apply delta

Fixes applied:
- NULLIF(col,'') on all type casts (empty strings → NULL)
- LTRIM(col,'0') on typeofdenomination (leading zeros)
- to_date(col, 'DD-MM-YYYY') for KBO date format
- Per-table SAVEPOINT (after staging, before upsert) with continue-on-failure
- Resume-from-table via last_merged_table
- reladiff integration for incremental merges
"""
import sys
import os
import logging
import csv
import io
from datetime import datetime
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '5434'))
DB_USER = os.getenv('DB_USER', 'aiuser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'aipassword123')
MASTER_DB = 'BE KBO MASTER'

MERGE_ORDER = ['enterprise', 'establishment', 'denomination',
               'address', 'contact', 'activity', 'branch', 'code']

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

LEADING_ZERO_COLS = {'typeofdenomination'}

CAST_MAP = {
    'int2': 'smallint', 'int4': 'integer', 'int8': 'bigint',
    'date': 'date', 'timestamp': 'timestamp',
    'bool': 'boolean', 'float4': 'real', 'float8': 'double precision',
    'numeric': 'numeric',
}


def cast_col(alias, col, pg_type, leading_zeros=False):
    if not pg_type:
        return f'{alias}."{col}"'
    inner = f"LTRIM({alias}.\"{col}\", '0')" if leading_zeros else f'{alias}."{col}"'
    nullif = f"NULLIF({inner}, '')"
    if pg_type == 'date':
        return f"to_date({nullif}, 'DD-MM-YYYY')"
    return f"{nullif}::{pg_type}"


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
            error_message TEXT, last_merged_table VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW())""")
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


def get_resume_tables(cur, label):
    cur.execute("SELECT last_merged_table FROM public.pipeline_state WHERE extract_version = %s", (label,))
    row = cur.fetchone()
    if row and row[0] and row[0] in MERGE_ORDER:
        idx = MERGE_ORDER.index(row[0]) + 1
        logger.info(f"Resuming after {row[0]}: starting from {MERGE_ORDER[idx]}")
        return MERGE_ORDER[idx:]
    return list(MERGE_ORDER)


def get_table_mapping(master_conn, version_conn, table_name, version_table):
    with version_conn.cursor() as cur:
        schema, tbl = version_table.split('.')
        cur.execute("""SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""", (schema, tbl))
        src_cols = [r[0] for r in cur.fetchall()]
    with master_conn.cursor() as cur:
        cur.execute("""SELECT column_name, udt_name FROM information_schema.columns
            WHERE table_schema = 'kbo_master' AND table_name = %s ORDER BY ordinal_position""", (table_name,))
        master_info = cur.fetchall()
        master_cols = [r[0] for r in master_info]
        master_types = {r[0]: r[1] for r in master_info}
    master_cols_lower = {c.lower(): c for c in master_cols}
    src_to_master = {}
    for sc in src_cols:
        if sc.lower() in master_cols_lower:
            src_to_master[sc] = master_cols_lower[sc.lower()]
    mapped_src = [c for c in src_cols if c in src_to_master]
    mapped_master = [src_to_master[c] for c in mapped_src]
    return mapped_src, mapped_master, master_types


def build_select_list(mapped_master, master_types):
    select_parts = []
    for c in mapped_master:
        udt = master_types.get(c, 'text')
        pg_type = CAST_MAP.get(udt)
        lz = c in LEADING_ZERO_COLS
        select_parts.append(cast_col('d', c, pg_type, leading_zeros=lz))
    return ', '.join(select_parts)


def merge_table(master_conn, version_conn, label, table_name, master_table, version_table, pkeys, master_types, mapped_master):
    with version_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {version_table}")
        source_count = cur.fetchone()[0]
    with master_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {master_table}")
        master_before = cur.fetchone()[0]
    logger.info(f"  Source: {source_count:,}, Master before: {master_before:,}")

    staging_table = f'public.kbo_merge_staging_{table_name}'
    with master_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
        col_defs = [f'"{c}" TEXT' for c in mapped_master]
        cur.execute(f"CREATE TABLE {staging_table} ({', '.join(col_defs)})")
    master_conn.commit()
    logger.info(f"  Created staging {staging_table}")

    staging_count = 0
    src_cols_quoted = [f'"{c}"' for c in mapped_master]
    with version_conn.cursor(f'stream_{table_name}') as vcur:
        vcur.execute(f"SELECT {', '.join(src_cols_quoted)} FROM {version_table}")
        while True:
            rows = vcur.fetchmany(100000)
            if not rows:
                break
            buf = io.StringIO()
            csv.writer(buf, quoting=csv.QUOTE_ALL).writerows(rows)
            buf.seek(0)
            with master_conn.cursor() as mcur:
                mcur.copy_expert(f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV)", buf)
            master_conn.commit()
            staging_count += len(rows)
            logger.info(f"  Staging: {staging_count:,}/{source_count:,}")
    logger.info(f"  Staged: {staging_count:,}")

    pk_list = ', '.join([f'"{pk}"' for pk in pkeys])
    pk_distinct = ', '.join([f's."{pk}"' for pk in pkeys])
    select_list = build_select_list(mapped_master, master_types)
    col_list = ', '.join([f'"{c}"' for c in mapped_master])

    non_pk_cols = [c for c in mapped_master if c not in pkeys]
    update_parts = [f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols]
    update_clause = ', '.join(update_parts)

    with master_conn.cursor() as cur:
        cur.execute(f"SAVEPOINT sp_upsert_{table_name}")

    logger.info(f"  Upserting (DISTINCT ON {pk_list})...")

    with master_conn.cursor() as cur:
        if update_clause:
            cur.execute(f"""INSERT INTO {master_table} ({col_list})
                SELECT {select_list} FROM (
                    SELECT DISTINCT ON ({pk_distinct}) *
                    FROM {staging_table} s
                ) d
                ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}""")
        else:
            cur.execute(f"""INSERT INTO {master_table} ({col_list})
                SELECT {select_list} FROM (
                    SELECT DISTINCT ON ({pk_distinct}) *
                    FROM {staging_table} s
                ) d
                ON CONFLICT ({pk_list}) DO NOTHING""")
        upserted = cur.rowcount
    master_conn.commit()
    logger.info(f"  Upserted: {upserted:,}")

    with master_conn.cursor() as cur:
        cur.execute(f"RELEASE SAVEPOINT sp_upsert_{table_name}")
    master_conn.commit()

    with master_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    master_conn.commit()

    with master_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {master_table}")
        master_after = cur.fetchone()[0]
    with master_conn.cursor() as cur:
        cur.execute("""INSERT INTO public.pipeline_metrics
            (extract_version, table_name, operation, rows_count)
            VALUES (%s, %s, 'MERGED', %s)""", (label, table_name, source_count))
    master_conn.commit()
    logger.info(f"  {master_table}: {source_count:,} processed -> {master_after:,} (was {master_before:,})")
    return source_count


def merge_extract(label):
    """Phase 1: Per-table upsert with continue-on-failure."""
    dbname = get_version_db(label)
    master_conn = get_conn(MASTER_DB)
    version_conn = get_conn(dbname)

    with master_conn.cursor() as cur:
        cur.execute("SET session_replication_role = 'replica';")
    master_conn.commit()

    with master_conn.cursor() as cur:
        cur.execute("SELECT status FROM public.pipeline_state WHERE extract_version = %s", (label,))
        row = cur.fetchone()
        if row and row[0] == 'merged':
            logger.info(f"Version {label} already merged, skipping")
            return False
    with master_conn.cursor() as cur:
        cur.execute("UPDATE public.pipeline_state SET status = 'merging', merge_started_at = NOW() WHERE extract_version = %s", (label,))
        master_conn.commit()

    with master_conn.cursor() as cur:
        tables_to_merge = get_resume_tables(cur, label)
    total_ops = 0
    failed_tables = []

    for csv_file, master_table, version_table in TABLES:
        table_name = master_table.split('.')[1]
        if table_name not in tables_to_merge:
            continue

        pkeys = PK_COLS.get(table_name, [])
        logger.info(f"Merging {master_table}...")

        try:
            _, mapped_master, master_types = get_table_mapping(
                master_conn, version_conn, table_name, version_table)
            logger.info(f"  Mapped {len(mapped_master)} cols: {mapped_master}")

            ops = merge_table(master_conn, version_conn, label, table_name,
                              master_table, version_table, pkeys, master_types, mapped_master)
            total_ops += ops

            with master_conn.cursor() as cur:
                cur.execute("UPDATE public.pipeline_state SET last_merged_table = %s WHERE extract_version = %s",
                            (table_name, label))
            master_conn.commit()
            logger.info(f"  {table_name}: OK")

        except Exception as e:
            logger.error(f"  {table_name}: FAILED - {e}")
            import traceback
            traceback.print_exc()
            try:
                master_conn.rollback()
            except Exception:
                pass
            failed_tables.append(table_name)
            logger.info(f"  Continuing to next table...")

    if not failed_tables:
        with master_conn.cursor() as cur:
            cur.execute("""UPDATE public.pipeline_state
                SET status = 'merged', merge_completed_at = NOW(), records_merged = %s
                WHERE extract_version = %s""", (total_ops, label))
            master_conn.commit()
        logger.info(f"Merge complete: {total_ops:,} total")
    else:
        with master_conn.cursor() as cur:
            cur.execute("""UPDATE public.pipeline_state
                SET status = 'partial', error_message = %s
                WHERE extract_version = %s""",
                (f"Failed: {', '.join(failed_tables)}", label))
            master_conn.commit()
        logger.warning(f"Merge partial: {len(failed_tables)} failed: {failed_tables}")

    with master_conn.cursor() as cur:
        cur.execute("SET session_replication_role = 'origin';")
    master_conn.commit()
    master_conn.close()
    version_conn.close()


def merge_incremental(label):
    """Phase 2: Use reladiff to identify only changed rows, then apply delta."""
    from reladiff import connect_to_table, diff_tables

    dbname = get_version_db(label)
    version_db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{dbname}"
    master_db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{MASTER_DB}"

    master_conn = get_conn(MASTER_DB)

    with master_conn.cursor() as cur:
        cur.execute("SET session_replication_role = 'replica';")
    master_conn.commit()

    with master_conn.cursor() as cur:
        cur.execute("SELECT status FROM public.pipeline_state WHERE extract_version = %s", (label,))
        row = cur.fetchone()
        if row and row[0] == 'merged':
            logger.info(f"Version {label} already merged, skipping")
            return False
    with master_conn.cursor() as cur:
        cur.execute("UPDATE public.pipeline_state SET status = 'merging', merge_started_at = NOW() WHERE extract_version = %s", (label,))
        master_conn.commit()

    total_upserts = 0
    total_deletes = 0

    for table_name in MERGE_ORDER:
        master_table = f"kbo_master.{table_name}"
        version_table = f"kbo.{table_name}"
        pkeys = tuple(PK_COLS.get(table_name, []))
        logger.info(f"Diffing {master_table}...")

        try:
            v_table = connect_to_table(version_db_url, version_table, pkeys)
            m_table = connect_to_table(master_db_url, master_table, pkeys)

            to_upsert = []
            to_delete = []

            for sign, row in diff_tables(v_table, m_table):
                if sign == '+':
                    to_upsert.append(row)
                elif sign == '-':
                    to_delete.append(row)

            logger.info(f"  {table_name}: {len(to_upsert):,} upserts, {len(to_delete):,} deletes")

            if to_delete:
                with master_conn.cursor() as cur:
                    for row in to_delete:
                        where = ' AND '.join([f'"{pk}" = %s' for pk in pkeys])
                        vals = tuple(row[pk] for pk in pkeys)
                        cur.execute(f"DELETE FROM {master_table} WHERE {where}", vals)
                master_conn.commit()
                logger.info(f"  Deleted {len(to_delete):,} rows")

            if to_upsert:
                _, mapped_master, master_types = get_table_mapping(
                    master_conn, get_conn(get_version_db(label)), table_name, version_table)
                col_list = ', '.join([f'"{c}"' for c in mapped_master])
                select_list = build_select_list(mapped_master, master_types)
                non_pk_cols = [c for c in mapped_master if c not in pkeys]
                update_parts = [f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols]
                update_clause = ', '.join(update_parts)

                staging_table = f'public.kbo_merge_staging_{table_name}'
                with master_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                    col_defs = [f'"{c}" TEXT' for c in mapped_master]
                    cur.execute(f"CREATE TABLE {staging_table} ({', '.join(col_defs)})")
                master_conn.commit()

                buf = io.StringIO()
                writer = csv.DictWriter(buf, fieldnames=mapped_master, quoting=csv.QUOTE_ALL, extrasaction='ignore')
                for row in to_upsert:
                    writer.writerow(row)
                buf.seek(0)
                with master_conn.cursor() as cur:
                    cur.copy_expert(f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV)", buf)
                master_conn.commit()

                pk_list = ', '.join([f'"{pk}"' for pk in pkeys])
                pk_distinct = ', '.join([f's."{pk}"' for pk in pkeys])
                with master_conn.cursor() as cur:
                    if update_clause:
                        cur.execute(f"""INSERT INTO {master_table} ({col_list})
                            SELECT {select_list} FROM (
                                SELECT DISTINCT ON ({pk_distinct}) *
                                FROM {staging_table} s
                            ) d
                            ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}""")
                    else:
                        cur.execute(f"""INSERT INTO {master_table} ({col_list})
                            SELECT {select_list} FROM (
                                SELECT DISTINCT ON ({pk_distinct}) *
                                FROM {staging_table} s
                            ) d
                            ON CONFLICT ({pk_list}) DO NOTHING""")
                    upserted = cur.rowcount
                master_conn.commit()

                with master_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                master_conn.commit()

                logger.info(f"  Upserted {upserted:,} rows")

            total_upserts += len(to_upsert)
            total_deletes += len(to_delete)

            with master_conn.cursor() as cur:
                cur.execute("""INSERT INTO public.pipeline_metrics
                    (extract_version, table_name, operation, rows_count)
                    VALUES (%s, %s, 'MERGED', %s)""", (label, table_name, len(to_upsert) + len(to_delete)))
            master_conn.commit()
            logger.info(f"  {table_name}: OK")

        except Exception as e:
            logger.error(f"  {table_name}: FAILED - {e}")
            import traceback
            traceback.print_exc()
            try:
                master_conn.rollback()
            except Exception:
                pass

    with master_conn.cursor() as cur:
        cur.execute("""UPDATE public.pipeline_state
            SET status = 'merged', merge_completed_at = NOW(), records_merged = %s
            WHERE extract_version = %s""", (total_upserts + total_deletes, label))
        master_conn.commit()
    logger.info(f"Incremental merge complete: {total_upserts:,} upserts, {total_deletes:,} deletes")

    with master_conn.cursor() as cur:
        cur.execute("SET session_replication_role = 'origin';")
    master_conn.commit()
    master_conn.close()


def show_status():
    conn = get_conn(MASTER_DB)
    print("\n" + "="*70)
    print("PIPELINE STATUS")
    print("="*70)
    with conn.cursor() as cur:
        cur.execute("""SELECT extract_version, status, records_loaded, records_merged,
            error_message, last_merged_table
            FROM public.pipeline_state ORDER BY extract_version""")
        for row in cur.fetchall():
            print(f"\nVersion {row[0]}: {row[1]}")
            print(f"  Loaded: {row[2] or 0:,} records")
            print(f"  Merged: {row[3] or 0:,} records")
            if row[4]:
                print(f"  Error: {row[4]}")
            if row[5]:
                print(f"  Last merged table: {row[5]}")
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
        incremental = '--incremental' in sys.argv
        logger.info(f"Pipeline for version {label}")
        logger.info(f"Extract path: {extract_path}")
        init_master()
        if not merge_only:
            load_extract(extract_path, label)
        if not load_only:
            if incremental:
                merge_incremental(label)
            else:
                merge_extract(label)
        show_status()
