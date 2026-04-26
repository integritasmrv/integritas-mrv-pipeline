#!/usr/bin/env python3
"""Fast merge ACTIVITY table only from BE KBO 276"""
import sys
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

sys.path.insert(0, '/opt/integritasmrv/company-register-pipeline')
from common.db_utils import DBConfig

class ActivityMerger:
    def __init__(self):
        self.c = DBConfig(host="127.0.0.1", port=5434)
    
    def cm(self):
        return psycopg2.connect(host=self.c.host, port=self.c.port, database="BE KBO MASTER", user=self.c.user, password=self.c.password)
    
    def cv(self, n):
        return psycopg2.connect(host=self.c.host, port=self.c.port, database=n, user=self.c.user, password=self.c.password)
    
    def merge_activity(self):
        logger.info("Starting ACTIVITY merge only")
        m = self.cm()
        v = self.cv("BE KBO 276")
        
        cs = v.cursor()
        cd = m.cursor()
        
        pk = ["entitynumber", "activitygroup", "naceversion", "nacecode"]
        pk_str = ", ".join(pk)
        
        cs.execute("SELECT * FROM kbo.activity LIMIT 1")
        cols = [x[0].lower() for x in cs.description]
        
        uc = [c for c in cols if c not in pk]
        us = ", ".join([c + "=EXCLUDED." + c for c in uc])
        cl = ", ".join(cols)
        ph = ", ".join(["%s"] * len(cols))
        sql = "INSERT INTO kbo_master.activity (" + cl + ") VALUES (" + ph + ") ON CONFLICT (" + pk_str + ") DO UPDATE SET " + us
        
        batch = 5000
        tot = off = 0
        while True:
            cs.execute("SELECT * FROM kbo.activity ORDER BY " + pk_str + " LIMIT " + str(batch) + " OFFSET " + str(off))
            rows = cs.fetchall()
            if not rows:
                break
            try:
                cd.executemany(sql, rows)
                m.commit()
                tot += len(rows)
            except Exception as e:
                m.rollback()
                logger.warning("Batch failed at %d, trying row-by-row", off)
                for row in rows:
                    try:
                        cd.execute(sql, row)
                        m.commit()
                        tot += 1
                    except:
                        m.rollback()
                        break
            off += batch
            if off % 100000 == 0:
                logger.info("  activity: %d", tot)
        
        logger.info("  activity: done (%d)", tot)
        m.close()
        v.close()

if __name__ == "__main__":
    ActivityMerger().merge_activity()
    logger.info("ACTIVITY MERGE COMPLETE")