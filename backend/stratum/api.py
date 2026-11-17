from contextlib import asynccontextmanager
import json,threading
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse,PlainTextResponse,JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel,Field
from . import engine as e
from .definitions import launch

@asynccontextmanager
async def lifespan(app):
    e.init()
    with e.connect() as c:exists=c.execute('select count(*) from partitions').fetchone()[0]
    if not exists:
        rid=e.new_run();threading.Thread(target=launch,args=(rid,),daemon=True).start()
    yield

app=FastAPI(title='STRATUM Data Reliability API',version='1.0.0',lifespan=lifespan)
SUBMIT_LOCK=threading.Lock()
class RunRequest(BaseModel):
    month: str='2025-01'
    mode: str='sample'
    scenario: str='normal'

@app.get('/api/health')
def health():return {'status':'ok'}
@app.get('/api/overview')
def overview():return e.overview()
@app.post('/api/runs',status_code=202)
def run(req:RunRequest):
    with SUBMIT_LOCK:
        with e.connect() as c:
            if c.execute("select count(*) from runs where status in ('queued','running')").fetchone()[0]:raise HTTPException(409,'A run is already active')
        try:rid=e.new_run(req.month,req.mode,req.scenario)
        except ValueError as err:raise HTTPException(422,str(err))
    threading.Thread(target=launch,args=(rid,),daemon=True).start()
    return {'id':rid}
@app.get('/api/table/{kind}')
def table(kind:str):
    if kind not in ['preview','schema','daily','zones']:raise HTTPException(404,'Unknown dataset')
    return e.query(kind)
@app.get('/api/runs/{run_id}')
def detail(run_id:str):
    with e.connect() as c:r=c.execute('select * from runs where id=?',(run_id,)).fetchone()
    if not r:raise HTTPException(404,'Run not found')
    result=e.run_record(r);result['quarantine_rows']=[]
    if result.get('quarantine'):
        with e.duckdb.connect() as db:
            result['quarantine_rows']=e.rows(db,f'select source_row,pickup,dropoff,distance,pickup_zone,total,reason from read_parquet({e.sqlstr(result["quarantine"])}) limit 50')
            result['reasons']=e.rows(db,f'select reason,count(*) as rows from read_parquet({e.sqlstr(result["quarantine"])}) group by 1 order by 2 desc')
    return result
@app.get('/api/runs/{run_id}/export')
def export(run_id:str):return JSONResponse(content=jsonable_encoder(detail(run_id)),headers={'Content-Disposition':f'attachment; filename="stratum-run-{run_id}.json"'})
@app.get('/api/runs/{run_id}/log',response_class=PlainTextResponse)
def log(run_id:str):
    with e.connect() as c:exists=c.execute('select 1 from runs where id=?',(run_id,)).fetchone()
    if not exists:raise HTTPException(404,'Run not found')
    p=e.HOME/'runs'/run_id/'dbt.log'
    return p.read_text()[-50000:] if p.exists() else 'No dbt log for this run.'
