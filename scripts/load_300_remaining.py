#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Load remaining BE KBO Extract 300 tables"""
import sys
import logging
import psycopg2
import csv
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

sys.path.insert(0, '/opt/integritasmrv/company-register-pipeline')
from common.db_utils import DBConfig

SOURCE = "/mnt/win_data/BE/KBO/ExtractNumber 300"
BATCH = 500

class Loader:
    def __init__(self):
        self.c = DBConfig(host="127.0.0.1", port=5434)
    
    def cv(self, n):
        return psycopg2.connect(host=self.c.host, port=self.c.port, database=n, user=self.c.user, password=self.c.password)
    
    def load_remaining(self):
        vd = "BE KBO 300"
        conn = self.cv(vd)
        
        tables = {
            "establishment": ["EstablishmentNumber", "EnterpriseNumber", "StartDate"],
            "denomination": ["EntityNumber", "Language", "Denomination", "TypeOfName", "FirstSeenVersion"],
            "address": ["EntityNumber", "TypeOfAddress", "CountryNL", "CountryFR", "Zipcode", "MunicipalityNL", "MunicipalityFR", "StreetNL", "StreetFR", "HouseNumber", "Box"],
            "contact": ["EntityNumber", "Language", "ContactCellular", "ContactEmail", "ContactPhone", "ContactWeb"],
            "activity": ["EntityNumber", "ActivityGroup", "NACEVersion", "NACECode", "Classification"],
            "branch": ["Id", "EnterpriseNumber", "StartDate"],
            "code": ["Category", "Code", "Language", "Description", "FirstSeenVersion", "LastUpdatedVersion", "LastSeenVersion"]
        }
        
        for tbl, cols in tables.items():
            csv_file = SOURCE + "/" + tbl + ".csv"
            if not os.path.exists(csv_file):
                logger.warning(csv_file + " not found, skipping")
                continue
            
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM kbo." + tbl)
            count = cur.fetchone()[0]
            if count > 0:
                logger.info(tbl + " already has " + str(count) + " rows, skipping")
                continue
            
            logger.info("Loading " + tbl + "...")
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = []
                for row in reader:
                    rows.append(row)
                    if len(rows) >= BATCH:
                        self._load_batch(conn, "kbo", tbl, rows, cols)
                        rows = []
                if rows:
                    self._load_batch(conn, "kbo", tbl, rows, cols)
            logger.info("  " + tbl + " loaded")
        
        conn.close()
        logger.info("LOAD COMPLETE")
    
    def _load_batch(self, conn, schema, table, rows, cols):
        if not rows:
            return
        cur = conn.cursor()
        col_names = ", ".join(['"' + c + '"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        sql = "INSERT INTO " + schema + "." + table + " (" + col_names + ") VALUES (" + placeholders + ") ON CONFLICT DO NOTHING"
        values = []
        for row in rows:
            vals = []
            for c in cols:
                v = row.get(c, '')
                if v == '' or v is None:
                    vals.append(None)
                else:
                    vals.append(v)
            values.append(vals)
        try:
            cur.executemany(sql, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e

if __name__ == "__main__":
    Loader().load_remaining()