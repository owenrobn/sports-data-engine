"""Offline invariants; no provider calls or database credentials needed."""
import sys, types, unittest
try:
    import fastapi, httpx
except ImportError:
    sys.modules['fastapi']=types.SimpleNamespace(HTTPException=Exception)
    sys.modules['fastapi.responses']=types.SimpleNamespace(HTMLResponse=str)
    sys.modules['httpx']=types.SimpleNamespace(HTTPStatusError=type('HTTPStatusError',(Exception,),{}))
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

if __name__=='__main__': unittest.main()

