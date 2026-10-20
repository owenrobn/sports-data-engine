import unittest
from datetime import datetime,timezone,timedelta
from engine_delivery import book_quality,fair_odds,live_fields
from bookmaker_feed import match_fixture,normalize
T=datetime(2026,10,20,10,tzinfo=timezone.utc)
class Quality(unittest.TestCase):
 def test_sorted_depth_and_insufficient_liquidity(self):
  b={'observed_at':T.isoformat(),'asks':[{'price':'.6','size':'5'},{'price':'.5','size':'2'}],'bids':[{'price':'.4','size':'10'}]}
  q=book_quality(b,T,10);self.assertEqual(q['best_ask'],.5);self.assertFalse(q['hypothetical_buy']['complete']);self.assertAlmostEqual(q['hypothetical_buy']['filled_notional'],4);self.assertGreater(q['hypothetical_buy']['price_slippage'],0)
 def test_crossed_or_future_book_rejected(self):
  b={'observed_at':(T+timedelta(seconds=1)).isoformat(),'asks':[{'price':'.5','size':'20'}],'bids':[{'price':'.6','size':'10'}]}
  self.assertFalse(book_quality(b,T)['usable']);self.assertTrue(book_quality(b,T)['crossed'])
 def odds(self,label,book='a',seconds=0): return {'id':label,'provider':'provider','bookmaker':book,'market_key':'h2h','selection_key':label,'line':None,'odds_decimal':2.1,'observed_at':(T-timedelta(seconds=seconds)).isoformat(),'effective_at':T.isoformat()}
 def test_no_partial_or_mixed_book_devig(self):
  self.assertEqual(fair_odds([self.odds('player1'),self.odds('player2','b')],['player1','player2'],T),[])
 def test_complete_synchronized_quotes(self):
  out=fair_odds([self.odds('player1'),self.odds('player2')],['player1','player2'],T)[0]
  self.assertTrue(out['usable']);self.assertAlmostEqual(sum(out['fair_probabilities'].values()),1);self.assertFalse(out['sharp_benchmark_verified'])
 def test_unknown_source_time_and_unsynchronized_quotes_rejected(self):
  a=self.odds('player1');b=self.odds('player2',seconds=8);a['effective_at']=None
  self.assertFalse(fair_odds([a,b],['player1','player2'],T)[0]['usable'])
 def test_missing_red_cards_never_zero(self):
  self.assertIn('red_cards',live_fields('football',{'participants':[{'score':'0'},{'score':'0'}]})['missing_fields'])
 def test_exact_fixture_matching_rejects_ambiguity(self):
  remote={'id':'r','home_team':'Alpha','away_team':'Beta','commence_time':T.isoformat()}
  e={'id':'e','starts_at':T.isoformat(),'metadata':{'participants':['Beta','Alpha']}}
  self.assertEqual(match_fixture(remote,[e]),e);self.assertIsNone(match_fixture(remote,[e,dict(e,id='other')]))
  remote['home_team']='A. Alpha';self.assertIsNone(match_fixture(remote,[e]))
 def test_tennis_odds_follow_canonical_player_order(self):
  r={'id':'r','home_team':'Alpha','away_team':'Beta','bookmakers':[{'key':'a','last_update':T.isoformat(),'markets':[{'key':'h2h','outcomes':[{'name':'Alpha','price':2.2},{'name':'Beta','price':1.8}]}]}]}
  e={'id':'e','metadata':{'participants':['Beta','Alpha']}}
  rows=normalize(r,e,'tennis',T.isoformat(),'https://example.com')
  self.assertEqual([x['selection_key'] for x in rows],['player2','player1']);self.assertNotIn('apiKey',str(rows))

class DeliveryIntegration(unittest.IsolatedAsyncioTestCase):
 async def test_both_sports_fail_closed_with_no_market_or_live_data(self):
  from unittest.mock import AsyncMock,patch
  import engine_delivery
  for sport in ('football','tennis'):
   context={'match':'Alpha vs Beta','decision_cutoff':T.isoformat(),'missing_required_inputs':['fresh_availability_confirmation'],'rae_requirements':{'sport':sport}}
   live={'sport':sport,'snapshot':None}
   engine_delivery.CTX['db']=AsyncMock(return_value=[])
   with patch('context_feed.package',AsyncMock(return_value=context)),patch('live_state.package',AsyncMock(return_value=live)):
    out=await engine_delivery.envelope('00000000-0000-0000-0000-000000000001',T.isoformat())
   self.assertFalse(out['engine_ready']);self.assertEqual(out['sport'],sport);self.assertIsNone(out['private_account_data']);self.assertIn('fresh_live_snapshot',out['blockers']);self.assertIn('verified_market_links_and_executable_books',out['blockers'])
 async def test_future_decision_rejected(self):
  import engine_delivery
  from fastapi import HTTPException
  from context_feed import clock
  with self.assertRaises(HTTPException): await engine_delivery.envelope('id',(clock()+timedelta(days=1)).isoformat())

class ResultIntegrity(unittest.TestCase):
 def test_draw_is_not_two_losses(self):
  from context_feed import scoreboard_records
  e={'id':'e','starts_at':T.isoformat(),'metadata':{}}
  p=[{'team':{'id':str(i),'displayName':n},'score':'1','winner':False} for i,n in enumerate(['Alpha','Beta'])]
  rows=scoreboard_records(e,'football',{'observed_at':T.isoformat(),'payload':{'competitors':p,'status':{'type':{'completed':True}}}})
  self.assertEqual([r['value']['result'] for r in rows if r['category']=='appearance'],['draw','draw'])
