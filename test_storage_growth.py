import unittest
from laboratory import closed_price_fingerprint
class StorageGrowth(unittest.TestCase):
 def market(self): return {'closed':True,'resolved':True,'clobTokenIds':'["a","b"]','outcomes':'["Yes","No"]','outcomePrices':'["1","0"]'}
 def test_unchanged_closed_prices_have_stable_identity(self):
  a=self.market();b=dict(a,updatedAt='later',volume='999');self.assertEqual(closed_price_fingerprint(a),closed_price_fingerprint(b))
 def test_corrections_and_outcome_mapping_changes_are_retained(self):
  a=self.market()
  for field,value in [('outcomePrices','["0","1"]'),('clobTokenIds','["b","a"]'),('outcomes','["No","Yes"]'),('resolved',False)]: self.assertNotEqual(closed_price_fingerprint(a),closed_price_fingerprint(dict(a,**{field:value})))
 def test_active_quotes_are_never_suppressed(self):
  self.assertIsNone(closed_price_fingerprint(dict(self.market(),closed=False)))

class ClosedPriceCollection(unittest.IsolatedAsyncioTestCase):
 async def test_retryable_writes_and_unchanged_closed_state(self):
  from unittest.mock import AsyncMock,patch
  import laboratory as lab
  market={'id':'10','closed':True,'outcomes':'["Yes","No"]','clobTokenIds':'["a","b"]','outcomePrices':'["1","0"]'}
  event={'id':'1','title':'Alpha vs Beta','markets':[market]}
  async def fetch(url,params=None): return {'events':[event]} if 'public-search' in url else []
  async def pages(table,**kwargs):
   return [{'market_id':'10','closed_price_fingerprint':closed_price_fingerprint(market)}] if table=='polymarket_market_catalog' else [{'provider_resource_id':'1'}]
  put=AsyncMock()
  with patch.dict(lab.CTX,{'classify_sport':lambda e:'football'}),patch.object(lab,'sid_for',AsyncMock(return_value='s')),patch.object(lab,'get',AsyncMock(return_value=[])),patch.object(lab,'pages',pages),patch.object(lab,'fetch',fetch),patch.object(lab,'put',put):
   await lab.catalog('football')
  self.assertFalse(any(c.args[0]=='prediction_market_snapshots' for c in put.call_args_list))
  self.assertTrue(any(c.args[0]=='polymarket_market_catalog' for c in put.call_args_list))
  async def pages_new(table,**kwargs): return [] if table=='polymarket_market_catalog' else [{'provider_resource_id':'1'}]
  async def fail_price(table,*args,**kwargs):
   if table=='prediction_market_snapshots': raise RuntimeError('storage unavailable')
  put=AsyncMock(side_effect=fail_price)
  with patch.dict(lab.CTX,{'classify_sport':lambda e:'football'}),patch.object(lab,'sid_for',AsyncMock(return_value='s')),patch.object(lab,'get',AsyncMock(return_value=[])),patch.object(lab,'pages',pages_new),patch.object(lab,'fetch',fetch),patch.object(lab,'put',put):
   with self.assertRaises(RuntimeError): await lab.catalog('football')
  self.assertFalse(any(c.args[0]=='polymarket_market_catalog' for c in put.call_args_list))
