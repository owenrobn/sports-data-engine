import copy
import unittest
from datetime import timedelta
from unittest.mock import patch,AsyncMock
from context_feed import clock
import fresh_markets as m

class MarketIntegrity(unittest.IsolatedAsyncioTestCase):
 def test_mapping_supports_versions_and_rejects_partial_or_duplicate(self):
  self.assertEqual(m.mapping({'clobTokenIds':'["1","2"]','outcomes':'["Yes","No"]'}),(['1','2'],['Yes','No']))
  self.assertEqual(m.mapping({'version':'v2','positionIds':['3','4'],'outcomes':['A','B']}),(['3','4'],['A','B']))
  self.assertIsNone(m.mapping({'clobTokenIds':['1','1'],'outcomes':['Yes','No']}))
  self.assertIsNone(m.mapping({'clobTokenIds':['1'],'outcomes':['Yes','No']}))
 async def test_partial_failure_is_recorded_and_never_usable(self):
  async def source(url,p):
   if p['token_id']=='2': raise RuntimeError('not available')
   return {'asset_id':'1','market':'condition','timestamp':str(clock().timestamp()*1000),'bids':[],'asks':[{'price':'0.5','size':'100'}]}
  market={'id':'3','conditionId':'condition','clobTokenIds':['1','2'],'outcomes':['Yes','No'],'mapping_observed_at':clock().isoformat()}
  with patch('fresh_markets.get_json',side_effect=source): receipt=await m.capture(market)
  out=m.assess(receipt,clock());self.assertFalse(out['executable_snapshot_usable']);self.assertEqual(len(receipt['errors']),1)
 def test_wrong_token_and_condition_are_rejected(self):
  for raw in ({'asset_id':'wrong','market':'c'},{'asset_id':'1','market':'wrong'}):
   with self.assertRaises(ValueError): m.normalized_book(raw,'1','c',clock().isoformat())
 async def test_stale_source_is_not_fresh_from_retrieval_alone(self):
  async def source(url,p): return {'asset_id':p['token_id'],'market':'c','timestamp':str((clock()-timedelta(hours=1)).timestamp()*1000),'bids':[],'asks':[{'price':'0.5','size':'100'}]}
  market={'id':'3','conditionId':'c','clobTokenIds':['1','2'],'outcomes':['Yes','No'],'mapping_observed_at':clock().isoformat()}
  with patch('fresh_markets.get_json',side_effect=source): receipt=await m.capture(market)
  out=m.assess(receipt,clock());self.assertTrue(out['all_outcomes_present']);self.assertFalse(out['executable_snapshot_usable']);self.assertTrue(out['quotes'][0]['fresh']);self.assertFalse(out['quotes'][0]['source_fresh'])
 async def test_only_exact_unambiguous_fixture_time_is_accepted(self):
  now=clock().isoformat();event={'id':'e','name':'Alpha vs Beta','league':'atp','starts_at':now,'participants':[{'name':'Alpha'},{'name':'Beta'}],'status':{}}
  market={'id':'m','closed':False,'gameStartTime':now,'conditionId':'c','clobTokenIds':['1','2'],'outcomes':['Yes','No']}
  raw={'events':[{'id':'remote','title':'Alpha vs Beta','markets':[market]}]}
  state={'packs':{'p':{'matches':[event]}}}
  with patch('fresh_markets.get_json',AsyncMock(return_value=raw)),patch('fresh_markets.capture',AsyncMock(side_effect=RuntimeError('book offline'))): await m.collect('tennis',state)
  self.assertEqual(len(state['markets']['searches']['e']['markets']),1)
  event2={**event,'id':'other'};state={'packs':{'p':{'matches':[event,event2]}}}
  with patch('fresh_markets.get_json',AsyncMock(return_value=raw)): await m.collect('tennis',state)
  self.assertEqual(state['markets']['searches']['e']['markets'],[])
