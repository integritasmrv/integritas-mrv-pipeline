#!/usr/bin/env python3
"""Merge remaining small tables from BE KBO 300"""
import sys
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

sys.path.insert(0, '/opt/integritasmrv/company-register-pipeline')
from common.db_utils import DBConfig

class Merge300Small:
    def __init__(self):
        self.c = DBConfig(host="127.0.0.1", port=5434)
    
    def cm(self):
        return psycopg2.connect(host=self.c.host, port=self.c.port, database="BE KBO MASTER", user=self.c.user, password=self.c.password)
    
    def cv(self, n):
        return psycopg2.connect(host=self.c.host, port=self.c.port, database=n, user=self.c.user, password=self.c.password)
    
    def merge_table(self, table, pk_cols):
        logger.info("Merging %s...", table)
        m = self.cm()
        v = self.cv("BE KBO 300")
        
        cs = v.cursor()
        cd = m.cursor()
        
        if isinstance(pk_cols, str):
            pk_cols = [pk_cols]
        
        pk_str = ", ".join(pk_cols)
        
        cs.execute("SELECT * FROM kbo." + table + " LIMIT 1")
        cols = [x[0].lower() for x in cs.description]
        
        uc = [c for c in cols if c not in pk_cols]
        us = ", ".join([c + "=EXCLUDED." + c for c in uc])
        cl = ", ".join(cols)
        ph = ", ".join(["%s"] * len(cols))
        sql = "INSERT INTO kbo_master." + table + " (" + cl + ") VALUES (" + ph + ") ON CONFLICT (" + pk_str + ") DO UPDATE SET " + us
        
        batch = 5000
        tot = off = 0
        while True:
            cs.execute("SELECT * FROM kbo." + table + " ORDER BY " + pk_str + " LIMIT " + str(batch) + " OFFSET " + str(off))
            rows = cs.fetchall()
            if not rows:
                break
            try:
                cd.executemany(sql, rows)
                m.commit()
                tot += len(rows)
            except Exception as e:
                m.rollback()
                for row in rows:
                    try:
                        cd.execute(sql, row)
                        m.commit()
                        tot += 1
                    except:
                        m.rollback()
                        break
            off += batch
            if off % 50000 == 0:
                logger.info("  %s: %d", table, tot)
        
        logger.info("  %s: done (%d)", table, tot)
        m.close()
        v.close()
    
    def merge_all(self):
        self.merge_table("contact", ["entitynumber", "entitycontact", "contacttype"])
        self.merge_table("branch", "id")
        self.merge_table("code", ["category", "code", "language"])
        self.merge_table("address", ["entitynumber", "typeofaddress"])
        logger.info("ALL DONE")

if __name__ == "__main__":
    Merge300Small().merge_all()