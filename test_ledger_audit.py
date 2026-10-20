import os
import unittest
from datetime import datetime,timedelta,timezone
from unittest.mock import AsyncMock,patch
from ledger_audit import resolution,brier,capture_audit,Forecast,install,authenticate
from fastapi import FastAPI,HTTPException
from fastapi.testclient import TestClient
T=datetime(2026,10,20,10,tzinfo=timezone.utc)
class LedgerIntegrity(unittest.TestCase):
 def market(self): return {'id':'1','conditionId':'c','closed':True,'umaResolutionStatus':'resolved','outcomes':'["Yes","No"]','clobTokenIds':'["a","b"]','outcomePrices':'["1","0"]'}
 def state(self): return {'condition_id':'c','status':'resolved','payouts':[1,0]}
 def test_agreeing_resolution_only(self):
  x=resolution(self.market(),self.state());self.assertTrue(x['scorable']);self.assertEqual(x['winning_token'],'a');self.assertFalse(x['on_chain_independently_verified'])
 def test_closed_alone_not_settlement(self):
  m=self.market();m['umaResolutionStatus']='proposed';self.assertFalse(resolution(m,self.state())['scorable'])
 def test_mismatch_and_split_payout_rejected(self):
  for state in ({'condition_id':'other','status':'resolved','payouts':[1,0]},{'condition_id':'c','status':'resolved','payouts':[0,1]},{'condition_id':'c','status':'resolved','payouts':[.5,.5]}): self.assertFalse(resolution(self.market(),state)['scorable'])
 def test_brier_conventions_and_bad_vectors(self):
  out=brier({'a':.8,'b':.2},{'a':1,'b':0});self.assertAlmostEqual(out['multiclass_sum'],.08);self.assertAlmostEqual(out['binary_scalar'],.04)
  for p,y in [({'a':.8,'b':.8},{'a':1,'b':0}),({'a':float('nan'),'b':0},{'a':1,'b':0}),({'a':.5,'b':.5},{'a':.5,'b':.5})]:
   with self.assertRaises(ValueError): brier(p,y)
 def capture(self): return {'id':1,'observed_at':T.isoformat(),'payload':{'market_id':'1','tokens':['a','b'],'outcomes':['Yes','No'],'capture_started_at':(T-timedelta(seconds=2)).isoformat(),'capture_finished_at':T.isoformat(),'phase':'scheduled_pre_10m'}}
 def book(self,t='a',seconds=1): return {'id':t,'market_id':'1','token_id':t,'observed_at':(T-timedelta(seconds=seconds)).isoformat(),'bids':[{'price':'.4','size':'20'}],'asks':[{'price':'.5','size':'20'}]}
 def test_all_tokens_required_and_no_invented_clv(self):
  c=self.capture();a=capture_audit([c],[self.book()],T+timedelta(minutes=5),T);self.assertEqual(a['usable_captures'],0)
  a=capture_audit([c],[self.book(),self.book('b')],T+timedelta(minutes=5),T)
  self.assertEqual(a['usable_captures'],1);self.assertIsNotNone(a['scheduled_pre_10m_capture']);self.assertFalse(a['actual_start_verified']);self.assertIsNone(a['closing_line_value'])
 def test_after_asof_and_outside_capture_excluded(self):
  self.assertEqual(capture_audit([self.capture()],[self.book(),self.book('b')],T,T-timedelta(seconds=1))['captures_observed'],0)
  self.assertEqual(capture_audit([self.capture()],[self.book(),self.book('b',seconds=3)],T,T)['usable_captures'],0)
 def test_private_endpoints_require_auth(self):
  app=FastAPI();install({'app':app,'SUPABASE_KEY':None});client=TestClient(app)
  with patch.dict(os.environ,{'SDE_ADMIN_TOKEN':''}):
   self.assertEqual(client.get('/data/ledger/private/00000000-0000-0000-0000-000000000001').status_code,403)
   payload={'forecast_id':'00000000-0000-0000-0000-000000000001','event_id':'00000000-0000-0000-0000-000000000001','market_id':'1','model':'rae','model_version':'1','inputs_as_of':T.isoformat(),'probabilities':{'a':.5,'b':.5}}
   self.assertEqual(client.post('/ledger/forecasts',json=payload).status_code,403)
 def test_forecast_rejects_extra_account_fields(self):
  with self.assertRaises(ValueError): Forecast(forecast_id='00000000-0000-0000-0000-000000000001',event_id='00000000-0000-0000-0000-000000000001',market_id='1',model='rae',model_version='1',inputs_as_of=T,probabilities={'a':.5,'b':.5},bankroll=100)

class ForecastArchive(unittest.IsolatedAsyncioTestCase):
 async def test_late_and_future_inputs_rejected(self):
  from ledger_audit import archive_forecast
  f=Forecast(forecast_id='00000000-0000-0000-0000-000000000001',event_id='00000000-0000-0000-0000-000000000001',market_id='1',model='rae',model_version='1',inputs_as_of=T,probabilities={'a':.5,'b':.5})
  with patch('ledger_audit.clock',return_value=T),patch('ledger_audit.get',AsyncMock(return_value=[{'starts_at':T.isoformat()}])):
   with self.assertRaises(HTTPException): await archive_forecast(f)
  f.inputs_as_of=T+timedelta(seconds=1)
  with patch('ledger_audit.clock',return_value=T):
   with self.assertRaises(HTTPException): await archive_forecast(f)
