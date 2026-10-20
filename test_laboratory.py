"""Offline invariants; no provider calls or database credentials needed."""
import sys, types, unittest
from unittest.mock import patch
try:
    import fastapi, httpx
except ImportError:
    sys.modules['fastapi']=types.SimpleNamespace(HTTPException=Exception)
    sys.modules['fastapi.responses']=types.SimpleNamespace(HTMLResponse=str)
    sys.modules['httpx']=types.SimpleNamespace(HTTPStatusError=type('HTTPStatusError',(Exception,),{}))
import laboratory
from unittest.mock import AsyncMock
from laboratory import match_candidate,parse_points,page

class Invariants(unittest.TestCase):
    def test_exact_pair_and_time(self):
        e={'name':'Serena Williams vs Angelique Kerber','starts_at':'2016-01-30T10:00:00Z','metadata':{}}
        m={'question':'Serena Williams vs Angelique Kerber','start_time':'2016-01-30T10:15:00Z'}
        self.assertTrue(match_candidate(e,m)['accepted'])
        m['start_time']='2016-02-01T10:15:00Z'
        self.assertFalse(match_candidate(e,m)['accepted'])
        m['start_time']=None
        self.assertFalse(match_candidate(e,m)['accepted'])
        m['question']='Serena Williams vs Venus Williams'
        self.assertIsNone(match_candidate(e,m))
    def test_serve_rotation_and_break_point(self):
        rows=parse_points('RRRSR;SSSS','A','B')
        self.assertEqual(rows[3]['server_name'],'A')
        self.assertTrue(rows[3]['is_break_point'])
        self.assertEqual(rows[5]['server_name'],'B')
        self.assertEqual(rows[5]['player2_games'],1)
        self.assertEqual(rows[5]['player1_points'],'0')
    def test_no_invented_point_time(self):
        rows=parse_points('SSSS;SSSS','A','B')
        self.assertTrue(all('effective_at' not in r for r in rows))
        with self.assertRaises(ValueError): parse_points('SSXS','A','B')
    def test_html_escape(self):
        self.assertNotIn('<h1><script>',page('<script>',''))

class LinkingWrites(unittest.IsolatedAsyncioTestCase):
    async def test_batches_keep_conservative_pair_time_rule(self):
        event={'id':'e','name':'Club A vs Club B','starts_at':'2026-10-20T12:00:00Z','metadata':{}}
        markets=[{'market_id':str(i),'polymarket_event_id':'p','condition_id':'c','question':'Club A vs Club B','event_title':'Club A vs Club B','market_type':'moneyline','start_time':'2026-10-20T12:00:00Z'} for i in range(201)]
        markets.append({**markets[0],'market_id':'wrong-date','start_time':'2026-10-10T12:00:00Z'})
        writer=AsyncMock()
        with patch.object(laboratory,'sid_for',AsyncMock(return_value='s')),patch.object(laboratory,'pages',AsyncMock(side_effect=[[event],markets])),patch.object(laboratory,'put',writer):
            self.assertEqual(await laboratory.link('football'),201)
        batches=[call.args for call in writer.call_args_list]
        self.assertTrue(all(len(args[1])<=100 for args in batches))
        accepted=[row for args in batches if args[0]=='market_match_links' for row in args[1]]
        self.assertEqual(len(accepted),201)
        self.assertNotIn('wrong-date',[row['market_id'] for row in accepted])

    def test_placeholder_never_links(self):
        e={'name':'TBD vs TBD','starts_at':'2026-10-20T10:00:00Z'}
        m={'question':'TBD vs TBD','start_time':e['starts_at']}
        self.assertIsNone(match_candidate(e,m))

class CollectorInvariants(unittest.IsolatedAsyncioTestCase):
    async def test_bad_tennis_row_is_preserved_and_next_match_imported(self):
        archived=[]; written=[]
        class Response:
            text='server1,server2,pbp,date,tournament,round\nA,B,SSRR;,2016-01-01,T,F\nC,D,SSSS,2016-01-01,T,F\n'
            def raise_for_status(self): pass
        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self,*a): pass
            async def get(self,*a,**kw): return Response()
        async def fake_get(*a,**kw): return []
        async def fake_put(table,rows,*a,**kw): written.append((table,rows))
        async def fake_archive(*a): archived.append(a)
        async def fake_canonical(*a): return 'test-event'
        async def fake_universe(*a): pass
        with patch.object(laboratory.httpx,'AsyncClient',lambda **kw:Client(),create=True), patch.object(laboratory,'get',fake_get), patch.object(laboratory,'put',fake_put), patch.object(laboratory,'canonical',fake_canonical), patch.object(laboratory,'universe',fake_universe), patch.dict(laboratory.CTX,{'raw_ingest':fake_archive,'emit':lambda *a,**kw:None}):
            self.assertEqual(await laboratory.tennis_points(),8)
        self.assertEqual(len(archived),2)
        self.assertEqual(archived[0][2]['row']['pbp'],'SSRR;')
        checkpoints=[rows for table,rows in written if table=='backfill_checkpoints']
        self.assertTrue(all(r['cursor_value']=='2' for r in checkpoints))

    async def test_pages_follow_all_small_batches(self):
        rows=list(range(251))
        async def fake_get(table,**q):
            return rows[q['offset']:q['offset']+q['limit']]
        with patch.object(laboratory,'get',fake_get):
            self.assertEqual(await laboratory.pages('events'),rows)

    async def test_database_outage_does_not_escape_collector(self):
        async def unavailable(*a,**kw): raise RuntimeError('offline')
        async def work(): self.fail('work must not run without run persistence')
        with patch.object(laboratory,'put',unavailable), patch.object(laboratory,'db',unavailable), patch.dict(laboratory.CTX,{'emit':lambda *a,**kw:None}):
            self.assertIsNone(await laboratory.tracked('espn','tennis',work))

if __name__=='__main__': unittest.main()




