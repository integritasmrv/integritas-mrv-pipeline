#!/usr/bin/env python3
import os,sys,json,logging,signal,time
import psycopg2,urllib.request
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s: %(message)s',handlers=[logging.FileHandler('/var/log/enrichment-listener.log'),logging.StreamHandler()])
log=logging.getLogger('enrichment_listener')
WINDMILL_WEBHOOK=os.environ.get('WINDMILL_WEBHOOK','https://enrichiq.integritasmrv.com/webhook/hubspot')
MRV={'host':'10.0.13.2','port':5432,'dbname':'integritasmrv_crm','user':'integritasmrv_crm_user','password':'Int3gr1t@smrv_S3cure_P@ssw0rd_2026'}
PWR={'host':'10.0.14.2','port':5432,'dbname':'poweriq_crm','user':'poweriq_crm_user','password':'P0w3r1Q_CRM_S3cur3_P@ss_2026'}
CRM_DBS={'MRV':MRV,'Power':PWR}
C={}
def g(ck):
  if ck not in C or C[ck].closed:
    c=CRM_DBS[ck]
    C[ck]=psycopg2.connect(host=c['host'],port=c['port'],dbname=c['dbname'],user=c['user'],password=c['password'])
  return C[ck]
def ce(t):
  if t=='nb_crm_contacts':return 'contact'
  if t=='nb_crm_customers':return 'company'
  return None
def tw(p):
  d=json.dumps(p).encode('utf-8')
  r=urllib.request.Request(WINDMILL_WEBHOOK,data=d,headers={'Content-Type':'application/json'},method='POST')
  try:
    with urllib.request.urlopen(r,timeout=60) as x:log.info('WM:%s',x.status);return True
  except Exception as e:log.error('WM err:%s',e);return False
def hn(ch,payload):
  parts=payload.split('|')
  if len(parts)<3:log.warning('Bad:%s',payload);return
  rid,hbid,ts=parts[0],parts[1],parts[2]
  log.info('rid=%s hbid=%s',rid,hbid)
  trig=0
  for ck in CRM_DBS:
    try:
      conn=g(ck);conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT);cur=conn.cursor()
      for tbl in ['nb_crm_contacts','nb_crm_customers']:
        cur.execute('SELECT id FROM '+tbl+' WHERE id=%s AND enrichment_status=%s',(int(rid),'To Be Enriched'))
        row=cur.fetchone()
        if row:
          et=ce(tbl);wp={'hubspot_id':hbid,'crm':ck,'entity_type':et,'record_id':str(rid),'timestamp':ts,'source':'pg_notify'}
          log.info('Trigger WM %s/%s',ck,et)
          if tw(wp):log.info('OK %s/%s',ck,et);trig+=1
          else:log.error('Fail %s/%s',ck,et);break
        cur.close()
    except Exception as e:log.error('Err %s:%s',ck,e)
  if trig==0:log.warning('No match rid=%s',rid)
def ln():
  for ck,c in CRM_DBS.items():
    conn=psycopg2.connect(host=c['host'],port=c['port'],dbname=c['dbname'],user=c['user'],password=c['password'])
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur=conn.cursor();cur.execute('LISTEN enrichment_queue');log.info('Listening %s',ck);C[ck]=conn
  log.info('Ready')
  while True:
    try:
      for ck in list(C.keys()):
        if C[ck].closed:log.info('Reconnect %s',ck);d=CRM_DBS[ck];C[ck]=psycopg2.connect(host=d['host'],port=d['port'],dbname=d['dbname'],user=d['user'],password=d['password']);C[ck].set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT);C[ck].cursor().execute('LISTEN enrichment_queue')
      c=C['MRV']
      if c.notifies:n=c.notifies.pop(0);log.info('Notify %s',n.payload);hn(n.channel,n.payload)
    except Exception as e:log.error('Loop:%s',e)
    time.sleep(0.1)
if __name__=='__main__':
  log.info('Start WINDMILL_WEBHOOK=%s',WINDMILL_WEBHOOK)
  def sd(s,f):log.info('Shutdown');[C[k].close() for k in list(C.keys()) if not C[k].closed];sys.exit(0)
  signal.signal(signal.SIGTERM,sd);signal.signal(signal.SIGINT,sd)
  try:ln()
  except KeyboardInterrupt:log.info('Interrupted');[C[k].close() for k in list(C.keys()) if not C[k].closed]
