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
