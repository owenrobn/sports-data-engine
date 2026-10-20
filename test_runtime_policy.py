import os
import unittest
from unittest.mock import AsyncMock,patch
from runtime_policy import policy
class FreePolicy(unittest.TestCase):
 def test_default_free_preserves_history_and_pauses_expansion(self):
  with patch.dict(os.environ,{},clear=True):
   p=policy();self.assertTrue(p['free_mode']);self.assertTrue(p['history_preserved']);self.assertFalse(p['archive_backfill_enabled']);self.assertFalse(p['deep_trade_backfill_enabled']);self.assertFalse(p['historical_fixture_expansion_enabled']);self.assertFalse(p['continuous_uptime_guaranteed'])
 def test_explicit_restore_is_available(self):
  with patch.dict(os.environ,{'SDE_FREE_MODE':'0'},clear=True):
   p=policy();self.assertTrue(p['archive_backfill_enabled']);self.assertTrue(p['deep_trade_backfill_enabled']);self.assertEqual(p['recent_history_fidelity_minutes'],1)

class RecentHistoryBudget(unittest.IsolatedAsyncioTestCase):
 async def test_each_sport_gets_same_bounded_recent_history_without_trade_backfill(self):
  import laboratory as lab
  for sport in ('football','tennis'):
   links=[{'event_id':str(i),'market_id':str(i),'condition_id':'condition','metadata':{'sport':sport}} for i in range(9)]
   async def pages(table,**kwargs): return links if table=='market_match_links' else [{'id':str(i)} for i in range(9)]
   async def get(table,**kwargs):
    if table=='backfill_checkpoints': return []
    return [{'market_id':'m','condition_id':'condition','outcomes':['Yes'],'clobTokenIds':['token']}]
   fetch=AsyncMock(return_value={'history':[]});trade=AsyncMock();book=AsyncMock(return_value=1)
   with patch.dict(os.environ,{},clear=True),patch.dict(lab.CTX,{'polymarket_book_snapshot':book,'polymarket_trades':trade}),patch.object(lab,'pages',pages),patch.object(lab,'get',get),patch.object(lab,'sid_for',AsyncMock(return_value='sport')),patch.object(lab,'fetch',fetch),patch.object(lab,'put',AsyncMock()):
    await lab.market_history(sport)
   self.assertEqual(fetch.await_count,6);trade.assert_not_awaited()
   for call in fetch.call_args_list:
    params=call.args[1];self.assertNotIn('interval',params);self.assertEqual(params['fidelity'],60);self.assertLessEqual(params['endTs']-params['startTs'],14*86400+1)
