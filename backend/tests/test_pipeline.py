import tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import duckdb
from stratum import engine as e

class PipelineTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.old=e.HOME;e.HOME=Path(self.temp.name);e.init()
 def tearDown(self):e.HOME=self.old;self.temp.cleanup()
 def record(self,rid):
  with e.connect() as c:return e.run_record(c.execute('select * from runs where id=?',(rid,)).fetchone())
 def test_sample_publish_idempotency_and_fail_closed(self):
  rid=e.new_run();e.execute(rid);r=self.record(rid)
  self.assertEqual(r['status'],'success',r.get('error'));self.assertEqual(r['raw'],50000)
  self.assertEqual(r['raw'],r['accepted']+r['rejected'])
  self.assertEqual(len(r['dbt_results']),13)
  self.assertTrue(all(x['status'] in ['pass','success'] for x in r['dbt_results']))
  original=e.query('daily');published=sum(x['trips'] for x in original)
  self.assertEqual(published,r['accepted'])
  same=e.new_run();e.execute(same);self.assertEqual(self.record(same)['status'],'skipped')
  self.assertEqual(e.query('daily'),original)
  for scenario in ['schema','quality']:
   bad=e.new_run(scenario=scenario);e.execute(bad)
   self.assertEqual(self.record(bad)['status'],'blocked')
   self.assertEqual(e.query('daily'),original)
  recovered=e.new_run();e.execute(recovered);self.assertEqual(self.record(recovered)['status'],'skipped')
 def test_dbt_failure_cannot_publish(self):
  with patch('stratum.engine.subprocess.run') as run:
   run.return_value.returncode=1;run.return_value.stdout='expected failure';run.return_value.stderr=''
   rid=e.new_run();e.execute(rid)
  self.assertEqual(self.record(rid)['status'],'failed');self.assertEqual(e.query('daily'),[])
  with e.connect() as c:self.assertEqual(c.execute('select count(*) from partitions').fetchone()[0],0)
 def test_quality_reasons_and_duplicate_reconciliation(self):
  con=duckdb.connect();source=e.SEED/'yellow_2025-01_sample.parquet'
  tmp=e.HOME/'fixture.parquet'
  con.execute(f"create table f as select * from read_parquet('{source}') where trip_distance>1 and total_amount>0 and total_amount<100 and tpep_dropoff_datetime>tpep_pickup_datetime and strftime(tpep_pickup_datetime,'%Y-%m')='2025-01' limit 1")
  con.execute('insert into f select * from f');con.execute('insert into f select * from f limit 1');con.execute('update f set trip_distance=-1 where rowid=2')
  con.execute(f"copy f to '{tmp}' (format parquet)");con.close()
  with duckdb.connect() as db:
   q=e.quality(db,tmp,'2025-01','normal');self.assertEqual(q['raw'],3);self.assertEqual(q['accepted'],1);self.assertEqual(q['rejected'],2)
   reasons=[x[0] for x in db.execute('select reason from rejected').fetchall()]
   self.assertIn('duplicate_candidate',reasons);self.assertIn('distance',reasons)
 def test_request_partition_validation(self):
  for args in [('2020-01','sample','normal'),('2025-02','sample','normal'),('2025-01','sample','arbitrary')]:
   with self.assertRaises(ValueError):e.new_run(*args)

if __name__=='__main__':unittest.main()
