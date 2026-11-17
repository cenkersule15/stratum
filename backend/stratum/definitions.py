import dagster as dg
from . import engine

class PartitionConfig(dg.Config):
    run_id: str

@dg.asset(group_name='reliability',description='Ingest, validate, run dbt and atomically publish one month. Detailed stages are in the control ledger.')
def warehouse_partition(config: PartitionConfig):
    engine.execute(config.run_id)
    with engine.connect() as con:
        r=engine.run_record(con.execute('select * from runs where id=?',(config.run_id,)).fetchone())
    if r['status'] in ['failed','blocked']:
        raise dg.Failure(description=r.get('error','Quality gate blocked publication'))
    return dg.MaterializeResult(metadata={'run_id':config.run_id,'status':r['status'],'accepted':r.get('accepted',0)})

defs=dg.Definitions(assets=[warehouse_partition])

def launch(run_id):
    try:
        dg.materialize([warehouse_partition],run_config={'ops':{'warehouse_partition':{'config':{'run_id':run_id}}}},raise_on_error=False)
    except Exception as e:
        engine.update(run_id,error=str(e));engine.status(run_id,'failed')
