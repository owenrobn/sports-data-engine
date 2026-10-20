import unittest
from datetime import datetime,timezone,timedelta
from live_state import snapshot,interpret
class LiveEvidence(unittest.TestCase):
 def test_no_inferred_environment_or_server(self):
  s=snapshot({'venue':{'name':'Clay Club'},'competitors':[{'team':{'id':'1'},'score':'0'}]},'2026-10-20T10:00:00Z','https://example.com')
  self.assertIsNone(s['surface']); self.assertIsNone(s['indoor']); self.assertIsNone(s['competitors'][0]['server'])
  self.assertEqual(s['competitors'][0]['score'],'0')
 def test_later_retrieval_excluded(self):
  with self.assertRaises(ValueError): interpret({'observed_at':'2026-10-20T10:01:00Z'},datetime(2026,10,20,10,tzinfo=timezone.utc))
 def test_freshness_is_not_latency(self):
  t=datetime(2026,10,20,10,tzinfo=timezone.utc)
  row={'observed_at':t.isoformat(),'payload':{}}
  self.assertTrue(interpret(row,t)['retrieval_fresh']);self.assertFalse(interpret(row,t)['provider_latency_known'])
  self.assertFalse(interpret(row,t+timedelta(minutes=3))['retrieval_fresh'])
