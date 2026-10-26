import copy
import unittest
from datetime import timedelta
from unittest.mock import patch,AsyncMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from context_feed import clock
import fresh_feed as f
import engine_delivery as e
import ledger_audit as l

class CloudDelivery(unittest.TestCase):
 def setUp(self):
  self.before=copy.deepcopy(f.STATE)
  self.eid='00000000-0000-0000-0000-000000000001'
  now=clock();self.now=now.isoformat()
  for sport in ('football','tennis'):
   f.STATE[sport]={'packs':{},'last_poll_at':self.now,'errors':[],'archive':{'verified':True},'history_cursor':1,'markets':{}}
  self.match={'id':self.eid,'sport':'tennis','name':'Alpha vs Beta','league':'atp','starts_at':(now+timedelta(hours=1)).isoformat(),'observed_at':self.now,'status':{'type':{'state':'pre'}},'participants':[{'source_id':'1','name':'Alpha','score':None},{'source_id':'2','name':'Beta','score':None}]}
  f.STATE['tennis']['packs']={'p':{'requested_date':now.date().isoformat(),'matches':[self.match]}}
 def tearDown(self): f.STATE.clear();f.STATE.update(self.before)
 def test_public_engine_and_ledger_work_without_database(self):
  app=FastAPI();database=AsyncMock(side_effect=AssertionError('database access'))
  e.install({'app':app,'db':database});l.install({'app':app,'db':database,'SUPABASE_KEY':''})
  with patch('runtime_policy.collection_paused',return_value=True),patch('ledger_audit.collection_paused',return_value=True):
   client=TestClient(app)
   for path in ('/data/engine/','/engine/','/data/ledger/','/ledger/'):
    r=client.get(path+self.eid);self.assertEqual(r.status_code,200,r.text)
   out=client.get('/data/engine/'+self.eid).json()
   self.assertFalse(out['engine_ready']);self.assertIsNone(out['private_account_data']);self.assertIn('complete_minutes:1',out['blockers'])
  database.assert_not_awaited()
 def test_historical_decision_never_uses_newer_live_observation(self):
  from fresh_engine import envelope
  out=envelope(self.eid,(clock()-timedelta(hours=1)).isoformat())
  self.assertIsNone(out['live_state']['snapshot']);self.assertFalse(out['live_state']['retrieval_fresh'])
  self.assertNotIn('participants',out['recent_performance']['match'])

class ArchiveReplay(unittest.IsolatedAsyncioTestCase):
 async def test_historical_archive_is_loaded_without_mutating_live_state(self):
  from fresh_engine import replay
  old=copy.deepcopy(f.STATE);now=clock();observed=(now-timedelta(minutes=12)).isoformat();decision=(now-timedelta(minutes=10)).isoformat();eid='00000000-0000-0000-0000-000000000003'
  match={'id':eid,'sport':'tennis','name':'Alpha vs Beta','league':'atp','starts_at':(now+timedelta(hours=1)).isoformat(),'observed_at':observed,'status':{},'participants':[{'source_id':'1','name':'Alpha'},{'source_id':'2','name':'Beta'}]}
  def snapshot(sport,cutoff):
   return {'schema_version':1,'sport':sport,'archived_at':observed,'last_poll_at':observed,'packs':{'p':{'requested_date':now.date().isoformat(),'matches':[match] if sport=='tennis' else []}}}
  with patch('fresh_feed.hydrate',side_effect=snapshot): out=await replay(eid,decision)
  self.assertEqual(out['historical_replay']['mode'],'verified_immutable_cloud_snapshot');self.assertEqual(out['live_state']['snapshot']['observed_at'],observed);self.assertEqual(f.STATE,old)
