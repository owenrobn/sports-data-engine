import copy
import gzip
import json
import unittest
from datetime import timedelta
from unittest.mock import patch,AsyncMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
import fresh_feed as f
from context_feed import clock

class Store:
 def __init__(self,limit=10**7,corrupt=False): self.limit=limit;self.data={};self.corrupt=corrupt
 def size(self): return sum(map(len,self.data.values()))
 def write_once(self,k,v):
  if k in self.data and self.data[k]!=v: raise ValueError('immutable conflict')
  self.data[k]=v
 def read(self,k): return b'bad' if self.corrupt and k.endswith('.gz') else self.data[k]

class FreshIntegrity(unittest.TestCase):
 def snapshot(self): return {'schema_version':1,'sport':'tennis','archived_at':clock().isoformat(),'packs':{'atp:today':{'matches':[{'id':'id','score':None}]}}}
 def test_private_archive_is_read_back_before_manifest(self):
  s=Store();snap=self.snapshot();receipt=f.archive_write(snap,s)
  self.assertTrue(receipt['verified']);keys=[k for k in s.data if k.endswith('.gz')];self.assertEqual(len(keys),1)
  self.assertEqual(json.loads(f.decompress(s.read(keys[0]))),snap);self.assertIn(keys[0]+'.manifest.json',s.data)
 def test_corruption_never_publishes_manifest(self):
  s=Store(corrupt=True)
  with self.assertRaises(ValueError): f.archive_write(self.snapshot(),s)
  self.assertFalse(any(k.endswith('manifest.json') for k in s.data))
 def test_byte_budget_stops_before_writing(self):
  s=Store(limit=1)
  with self.assertRaises(RuntimeError): f.archive_write(self.snapshot(),s)
  self.assertEqual(s.data,{})
 def test_decompression_is_bounded(self):
  with patch.object(f,'MAX_OBJECT',100):
   with self.assertRaises(ValueError): f.decompress(gzip.compress(b'x'*101))
 def test_missing_fields_not_inferred_and_tbd_excluded(self):
  comp={'id':'1','date':clock().isoformat(),'competitors':[{'athlete':{'id':'a','displayName':'A'},'score':'0'},{'athlete':{'id':'b','displayName':'B'}}]}
  r=f.normalize({},comp,'tennis','atp',clock().isoformat())
  self.assertEqual(r['participants'][0]['score'],'0');self.assertIsNone(r['surface']);self.assertIsNone(r['participants'][1]['score']);self.assertFalse(r['provider_latency_known'])
  comp['competitors'][0]['athlete']['displayName']='TBD';self.assertIsNone(f.normalize({},comp,'tennis','atp',clock().isoformat()))
 def test_stale_archive_does_not_become_fresh_after_loading(self):
  before=copy.deepcopy(f.STATE['tennis'])
  try:
   old=(clock()-timedelta(hours=1)).isoformat();f.STATE['tennis']['packs']={'p':{'requested_date':clock().date().isoformat(),'matches':[{'id':'id','observed_at':old,'starts_at':None,'status':{}}]}}
   out=f.output('tennis');self.assertFalse(out['matches'][0]['retrieval_fresh']);self.assertFalse(out['engine_ready'])
  finally: f.STATE['tennis']=before
 def test_public_api_exposes_no_bucket_credentials(self):
  app=FastAPI();f.install(app);c=TestClient(app)
  r=c.get('/data/fresh?sport=tennis');self.assertEqual(r.status_code,200);self.assertNotIn('ACCESS_KEY',r.text);self.assertEqual(c.get('/data/fresh?sport=wrong').status_code,422)

class SourceIsolation(unittest.IsolatedAsyncioTestCase):
 async def test_partial_source_failure_does_not_block_archive(self):
  before=copy.deepcopy(f.STATE['tennis'])
  async def source(url,params):
   if '/wta/' in url: raise RuntimeError('unavailable')
   return {'events':[]}
  try:
   with patch('fresh_feed.get_json',side_effect=source),patch('fresh_feed.persist',return_value={'verified':True,'status':'verified'}): await f.poll('tennis')
   self.assertTrue(f.STATE['tennis']['archive']['verified']);self.assertEqual(len(f.STATE['tennis']['errors']),3);self.assertTrue(any(k.startswith('atp:') for k in f.STATE['tennis']['packs']))
  finally: f.STATE['tennis']=before

class RecoveryStartup(unittest.TestCase):
 def test_paused_service_can_start_and_stop_without_database_access(self):
  import main
  with patch('main.SUPABASE_KEY','configured'),patch('main.db',AsyncMock(side_effect=AssertionError('database must remain untouched'))) as database,patch('fresh_feed.hydrate',return_value=None),patch('fresh_feed.poll',AsyncMock()),patch('archive_health.probe',return_value={'status':'connected','checked_at':clock().isoformat(),'historical_export_verified':False}):
   with TestClient(main.app) as client:
    r=client.get('/health');self.assertEqual(r.status_code,200);self.assertTrue(r.json()['database_collection_paused']);self.assertFalse(r.json()['database_verified'])
    self.assertEqual(client.get('/fresh?sport=football').status_code,200);self.assertEqual(client.get('/fresh?sport=tennis').status_code,200)
   database.assert_not_awaited()

class SourcePresentation(unittest.TestCase):
 def test_set_games_are_displayed_without_inventing_match_score(self):
  m={'sport':'tennis','status':{'type':{'state':'in'}},'participants':[{'score':None,'sets':[{'value':4},{'value':1}]},{'score':None,'sets':[{'value':6},{'value':0}]}]}
  self.assertEqual(f.score_display(m),'4 – 6 | 1 – 0 (set games)')
  m['status']['type']['state']='pre';self.assertEqual(f.score_display(m),'Not started')
 def test_duplicate_source_queries_do_not_multiply_archive_rows(self):
  m={'id':'a','observed_at':'2026-10-26T10:00:00Z'};n=dict(m,observed_at='2026-10-26T10:01:00Z')
  out=f.compact_packs({'first':{'matches':[m]},'second':{'matches':[n]}})
  self.assertEqual(sum(len(p['matches']) for p in out.values()),1);self.assertEqual(out['first']['source_matches_returned'],1);self.assertEqual(out['second']['matches'],[n])
 def test_unverified_start_does_not_produce_pre_match_windows(self):
  from fastapi import HTTPException
  with patch('fresh_feed.output',return_value={'matches':[{'id':'unknown','starts_at':None}]}):
   with self.assertRaises(HTTPException): f.match_package('unknown')

class RestartProgress(unittest.TestCase):
 def test_archive_restore_resumes_history_cursor_without_refreshing_observations(self):
  old=copy.deepcopy(f.STATE['tennis']);observed=(clock()-timedelta(hours=1)).isoformat()
  try:
   snapshot={'history_cursor':8,'last_poll_at':observed,'archived_at':observed,'packs':{'p':{'requested_date':clock().date().isoformat(),'matches':[{'id':'a','observed_at':observed,'starts_at':None,'status':{}}]}}}
   f.restore_state('tennis',snapshot)
   self.assertEqual(f.STATE['tennis']['history_cursor'],8);self.assertEqual(f.STATE['tennis']['last_poll_at'],observed);self.assertFalse(f.output('tennis')['matches'][0]['retrieval_fresh'])
  finally: f.STATE['tennis']=old

class ArchiveRequestBudget(unittest.TestCase):
 def test_bucket_scan_is_cached_and_own_writes_count_towards_budget(self):
  store=Store();usage={'bytes':None,'checked_at':None};wrapper=f.BudgetedStore(store,usage)
  with patch.object(store,'size',wraps=store.size) as scans:
   self.assertEqual(wrapper.size(),0);wrapper.write_once('data',b'123')
   self.assertEqual(wrapper.size(),3);self.assertEqual(wrapper.size(),3);self.assertEqual(scans.call_count,1)
   with patch('fresh_feed.clock',return_value=usage['checked_at']+timedelta(hours=2)): self.assertEqual(wrapper.size(),3)
   self.assertEqual(scans.call_count,2)

class IndependentDetails(unittest.IsolatedAsyncioTestCase):
 async def test_both_sports_collect_and_cache_timestamped_receipts(self):
  for sport,league in [('football','eng.1'),('tennis','atp')]:
   m={'id':'a','league':league,'provider_match_id':'123','provider_event_id':'456','starts_at':clock().isoformat(),'status':{}}
   state={'packs':{'p':{'matches':[m]}}}
   raw={'rosters':[{'athlete':{'height':180}}]} if sport=='football' else {'surface':'clay','competitors':[]}
   with patch('fresh_feed.get_json',AsyncMock(return_value=raw)) as source:
    await f.enrich(sport,state);await f.enrich(sport,state)
    self.assertEqual(source.await_count,1)
   receipt=state['details']['a'];self.assertEqual(receipt['payload'],raw);self.assertIsNone(receipt['source_updated_at']);self.assertFalse(receipt['independent_fixture_verification'])
 async def test_detail_failure_preserves_original_observation(self):
  old=(clock()-timedelta(days=1)).isoformat()
  m={'id':'a','league':'eng.1','provider_match_id':'123','status':{}}
  state={'packs':{'p':{'matches':[m]}},'details':{'a':{'observed_at':old,'payload':{'rosters':[]}}}}
  with patch('fresh_feed.get_json',AsyncMock(side_effect=RuntimeError('source failed'))): await f.enrich('football',state)
  self.assertEqual(state['details']['a']['observed_at'],old);self.assertEqual(state['details']['a']['last_error_class'],'RuntimeError')
 async def test_oversized_detail_is_rejected_without_blocking_lane(self):
  m={'id':'a','league':'eng.1','provider_match_id':'123','status':{}}
  state={'packs':{'p':{'matches':[m]}}}
  with patch('fresh_feed.get_json',AsyncMock(return_value={'rosters':['x'*140000]})): await f.enrich('football',state)
  self.assertNotIn('payload',state['details']['a'])
