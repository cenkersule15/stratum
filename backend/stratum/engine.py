from __future__ import annotations
import hashlib, json, math, os, sqlite3, subprocess, sys, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import duckdb

ROOT=Path(__file__).resolve().parents[2]
HOME=Path(os.environ.get('STRATUM_HOME',ROOT/'var'))
SEED=ROOT/'data/seed'
MONTHS=['2025-01','2025-02','2025-03']
POLICY='yellow-v1.0'
LOCK=threading.RLock()
REQUIRED=['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance','PULocationID','DOLocationID','fare_amount','total_amount','payment_type']
def now():return datetime.now(timezone.utc).isoformat()
def digest(path):
 h=hashlib.sha256()
 with open(path,'rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def sqlstr(s):return "'"+str(s).replace("'","''")+"'"
def connect():
 HOME.mkdir(parents=True,exist_ok=True)
 c=sqlite3.connect(HOME/'control.sqlite',timeout=30);c.row_factory=sqlite3.Row
 c.execute('pragma journal_mode=WAL');return c
def init():
 for name in ['bronze','silver','quarantine','runs','warehouse']: (HOME/name).mkdir(parents=True,exist_ok=True)
 with connect() as c:
  c.executescript('''create table if not exists runs(id text primary key, month text, mode text, scenario text, status text, created_at text, finished_at text, payload text);
  create table if not exists partitions(month text primary key, sha text, policy text, silver text, run_id text, rows integer, mode text);
  create table if not exists settings(key text primary key,value text);''')
  c.execute("update runs set status='interrupted',finished_at=? where status in ('queued','running')",(now(),))
 (HOME/'dagster').mkdir(exist_ok=True)
 (HOME/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
 os.environ.setdefault('DAGSTER_HOME',str(HOME/'dagster'))
def update(run_id,**values):
 with connect() as c:
  row=c.execute('select payload from runs where id=?',(run_id,)).fetchone()
  payload=json.loads(row['payload']);payload.update(values)
  c.execute('update runs set payload=? where id=?',(json.dumps(payload),run_id))
def status(run_id,state):
 with connect() as c:c.execute('update runs set status=?,finished_at=? where id=?',(state,now() if state not in ['queued','running'] else None,run_id))
def event(run_id,step,state,**details):
 with connect() as c:
  row=c.execute('select payload from runs where id=?',(run_id,)).fetchone();p=json.loads(row['payload'])
  p.setdefault('events',[]).append({'step':step,'status':state,'at':now(),**details})
  p['step']=step;c.execute('update runs set payload=? where id=?',(json.dumps(p),run_id))
def new_run(month='2025-01',mode='sample',scenario='normal'):
 if month not in MONTHS or mode not in ['sample','full'] or scenario not in ['normal','schema','quality']:raise ValueError('Unsupported partition, mode or scenario')
 if mode=='sample' and month!='2025-01':raise ValueError('Bundled sample covers January 2025 only; select full mode for other months')
 rid=uuid.uuid4().hex[:12]
 with connect() as c:c.execute('insert into runs values(?,?,?,?,?,?,?,?)',(rid,month,mode,scenario,'queued',now(),None,json.dumps({'events':[]}),))
 return rid

def source(month,mode):
 provenance=json.loads((SEED/'provenance.json').read_text())
 if digest(SEED/'zone_lookup.csv')!=provenance['zone_sha256']:raise ValueError('Zone reference checksum mismatch')
 if mode=='sample':
  path=SEED/'yellow_2025-01_sample.parquet';provenance=json.loads((SEED/'provenance.json').read_text())
  if digest(path)!=provenance['sample_sha256']:raise ValueError('Bundled sample checksum mismatch')
  return path,provenance['url']
 path=HOME/'bronze'/f'yellow_tripdata_{month}.parquet'
 url=f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet'
 if not path.exists():
  tmp=path.with_suffix('.download')
  try:
   for attempt in range(3):
    try:
     with urlopen(url,timeout=90) as response,open(tmp,'wb') as out:
      total=0
      while b:=response.read(1024*1024):
       total+=len(b)
       if total>500*1024*1024:raise ValueError('Source exceeds 500 MiB safety limit')
       out.write(b)
     duckdb.connect().execute(f'select count(*) from read_parquet({sqlstr(tmp)})').fetchone()
     tmp.replace(path);break
    except Exception:
     tmp.unlink(missing_ok=True)
     if attempt==2:raise
     time.sleep(attempt+1)
  finally:tmp.unlink(missing_ok=True)
 if month=='2025-01' and digest(path)!=provenance['source_sha256']:raise ValueError('Pinned January source checksum mismatch')
 return path,url

def quality(con,path,month,scenario):
 con.execute(f'create table incoming as select * from read_parquet({sqlstr(path)})')
 schema=[{'name':r[0],'type':r[1]} for r in con.execute('describe incoming').fetchall()]
 if scenario=='schema':con.execute('alter table incoming drop column total_amount')
 missing=sorted(set(REQUIRED)-{r[0] for r in con.execute('describe incoming').fetchall()})
 if missing:return {'schema':schema,'missing':missing}
 if scenario=='quality':con.execute('update incoming set trip_distance=-999')
 con.execute(f"create table zones as select LocationID::integer zone_id, Borough borough, Zone as zone_name from read_csv_auto({sqlstr(SEED/'zone_lookup.csv')})")
 con.execute('''create table typed as select row_number() over () source_row,
 md5(to_json(incoming)) record_key,
 try_cast(tpep_pickup_datetime as timestamp) pickup,
 try_cast(tpep_dropoff_datetime as timestamp) dropoff,
 try_cast(trip_distance as double) distance,
 try_cast(PULocationID as integer) pickup_zone,
 try_cast(DOLocationID as integer) dropoff_zone,
 try_cast(fare_amount as decimal(12,2)) fare,
 try_cast(total_amount as decimal(12,2)) total,
 try_cast(payment_type as integer) payment_type from incoming''')
 rules={
  'time_order':"pickup is null or dropoff is null or dropoff<=pickup or date_diff('second',pickup,dropoff)>21600",
  'partition':f"pickup is null or strftime(pickup,'%Y-%m')!={sqlstr(month)}",
  'distance':'distance is null or not isfinite(distance) or distance<=0 or distance>200',
  'zone':'pickup_zone not in (select zone_id from zones) or dropoff_zone not in (select zone_id from zones) or pickup_zone is null or dropoff_zone is null',
  'fare':'fare is null or total is null or fare<0 or total<0 or total>1000',
  'payment':'payment_type is null or payment_type not between 0 and 6'
 }
 checks=[{'rule':k,'failed':con.execute(f'select count(*) from typed where {v}').fetchone()[0]} for k,v in rules.items()]
 reason=','.join(f'case when {v} then {sqlstr(k)} end' for k,v in rules.items())
 con.execute(f"create table checked as select *,concat_ws('|',{reason}) reason from typed")
 con.execute("create table classified as select *,row_number() over(partition by record_key order by source_row) duplicate_rank from checked")
 con.execute("create table accepted as select *,strftime(pickup,'%Y-%m') partition_month from classified where reason='' and duplicate_rank=1")
 con.execute("create table rejected as select * exclude(reason),case when reason='' then 'duplicate_candidate' else reason end reason from classified where reason!='' or duplicate_rank>1")
 raw=con.execute('select count(*) from incoming').fetchone()[0];accepted=con.execute('select count(*) from accepted').fetchone()[0];rejected=con.execute('select count(*) from rejected').fetchone()[0]
 assert raw==accepted+rejected
 return {'schema':schema,'missing':[],'raw':raw,'accepted':accepted,'rejected':rejected,'checks':checks,'pass_rate':accepted/raw if raw else 0}

def execute(run_id):
 with LOCK:
  started=time.perf_counter();status(run_id,'running')
  with connect() as c:r=dict(c.execute('select * from runs where id=?',(run_id,)).fetchone())
  folder=HOME/'runs'/run_id;folder.mkdir(parents=True,exist_ok=True)
  con=None
  try:
   event(run_id,'ingest','running');path,url=source(r['month'],r['mode']);sha=digest(path)
   update(run_id,source_sha256=sha,source_url=url,bytes=path.stat().st_size,policy=POLICY)
   event(run_id,'ingest','passed',sha256=sha)
   with connect() as c:old=c.execute('select * from partitions where month=?',(r['month'],)).fetchone()
   if old and old['sha']==sha and old['policy']==POLICY and r['scenario']=='normal':
    event(run_id,'publish','skipped',reason='Same content hash and policy already published');status(run_id,'skipped');return
   con=duckdb.connect(str(folder/'validation.duckdb'));con.execute("set memory_limit='1GB'");con.execute('set threads=1');con.execute('set preserve_insertion_order=false')
   event(run_id,'contract','running');q=quality(con,path,r['month'],r['scenario']);update(run_id,**q)
   if q['missing']:
    event(run_id,'contract','blocked',missing=q['missing']);status(run_id,'blocked');return
   event(run_id,'contract','passed',columns=len(q['schema']));event(run_id,'quality','running')
   quarantine=HOME/'quarantine'/f'{run_id}.parquet'
   con.execute(f'copy rejected to {sqlstr(quarantine)} (format parquet,compression zstd)');update(run_id,quarantine=str(quarantine))
   if q['raw']==0 or q['pass_rate']<.8:
    event(run_id,'quality','blocked',pass_rate=q['pass_rate']);status(run_id,'blocked');return
   event(run_id,'quality','passed',accepted=q['accepted'],rejected=q['rejected'])
   silver=HOME/'silver'/f'{run_id}.parquet';con.execute(f'copy accepted to {sqlstr(silver)} (format parquet,compression zstd)');con.close();con=None
   event(run_id,'transform','running')
   warehouse=HOME/'warehouse'/f'{run_id}.duckdb'
   with connect() as c:parts={x['month']:x['silver'] for x in c.execute('select * from partitions')}
   parts[r['month']]=str(silver)
   db=duckdb.connect(str(warehouse));db.execute("set memory_limit='1GB'");db.execute('set threads=1');db.execute('set preserve_insertion_order=false');db.execute('create schema raw')
   paths='['+','.join(sqlstr(p) for p in parts.values())+']'
   db.execute(f'create table raw.trips as select * from read_parquet({paths},union_by_name=true)')
   db.execute(f"create table raw.zones as select LocationID::integer zone_id,Borough borough,Zone as zone_name from read_csv_auto({sqlstr(SEED/'zone_lookup.csv')})");db.close()
   env={**os.environ,'STRATUM_WAREHOUSE':str(warehouse),'DBT_SEND_ANONYMOUS_USAGE_STATS':'false'}
   cmd=[str(Path(sys.executable).parent/'dbt'),'build','--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(ROOT/'dbt'),'--target-path',str(folder/'dbt-target'),'--log-path',str(folder/'dbt-logs')]
   result=subprocess.run(cmd,env=env,capture_output=True,text=True,timeout=180)
   (folder/'dbt.log').write_text(result.stdout+'\n'+result.stderr)
   if result.returncode:raise RuntimeError('dbt build failed; see run log')
   run_results=json.loads((folder/'dbt-target/run_results.json').read_text());tests=[{'name':x['unique_id'],'status':x['status'],'seconds':x['execution_time']} for x in run_results['results']]
   update(run_id,dbt_results=tests);event(run_id,'transform','passed',tests=len(tests))
   event(run_id,'publish','running')
   with connect() as c:
    c.execute('insert or replace into partitions values(?,?,?,?,?,?,?)',(r['month'],sha,POLICY,str(silver),run_id,q['accepted'],r['mode']))
    c.execute("insert or replace into settings values('warehouse',?)",(str(warehouse),))
   event(run_id,'publish','passed');status(run_id,'success')
  except Exception as e:
   update(run_id,error=str(e));event(run_id,'pipeline','failed',error=str(e));status(run_id,'failed')
  finally:
   if con:con.close()
   update(run_id,duration_ms=round((time.perf_counter()-started)*1000,2))

def run_record(r):
 d=dict(r);d.update(json.loads(d.pop('payload')));return d
def rows(con,sql,args=None):
 cur=con.execute(sql,args or []);names=[c[0] for c in cur.description]
 return [dict(zip(names,[None if isinstance(v,float) and not math.isfinite(v) else v for v in r])) for r in cur.fetchall()]
def query(kind,limit=50):
 with connect() as c:r=c.execute("select value from settings where key='warehouse'").fetchone()
 if not r:return []
 with duckdb.connect(r['value'],read_only=True) as db:
  queries={'daily':'select * from main.daily_metrics order by day','zones':'select * from main.zone_metrics order by trips desc limit 12','preview':f'select record_key,partition_month,source_row,pickup,dropoff,distance,pickup_zone,dropoff_zone,total,payment_type from raw.trips order by pickup limit {min(limit,100)}','schema':'describe raw.trips'}
  return rows(db,queries[kind])
def overview():
 with connect() as c:
  runs=[run_record(r) for r in c.execute('select * from runs order by created_at desc limit 40')]
  parts=[dict(r) for r in c.execute('select * from partitions order by month')]
 daily=query('daily');zones=query('zones')
 return {'runs':runs,'partitions':parts,'daily':daily,'zones':zones,'months':MONTHS,'policy':POLICY,'source':json.loads((SEED/'provenance.json').read_text()),'assets':[
  {'id':'bronze','label':'source.parquet','layer':'bronze','deps':[]},
  {'id':'contract','label':'schema_contract','layer':'control','deps':['bronze']},
  {'id':'silver','label':'validated_trips','layer':'silver','deps':['contract']},
  {'id':'quarantine','label':'quarantined_rows','layer':'control','deps':['contract']},
  {'id':'staging','label':'stg_trips','layer':'silver','deps':['silver']},
  {'id':'daily','label':'daily_metrics','layer':'gold','deps':['staging']},
  {'id':'zones','label':'zone_metrics','layer':'gold','deps':['staging']}]}
