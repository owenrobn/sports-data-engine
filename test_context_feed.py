import unittest
from datetime import datetime,timedelta,timezone
from unittest.mock import patch
import context_feed as c

T=datetime(2026,10,20,12,tzinfo=timezone.utc)
KEY='espn:tennis:1'

def row(category='appearance',metric='completed_match',value=None,days=2,observed=None,event='prior',**kw):
    return c.record('tennis',KEY,'Test Player',category,metric,value if value is not None else {'result':'win','match_duration_minutes':60},'test','https://example.org/source',effective=(T-timedelta(days=days)).isoformat(),observed=observed or (T-timedelta(hours=1)).isoformat(),event_id=event,**kw)

class EvidenceRules(unittest.TestCase):
    def test_labelled_unit_conversion(self):
        self.assertAlmostEqual(c.measurement("6'3\"",'height'),190.5)
        self.assertEqual(c.measurement('1.85 m','height'),185)
        self.assertAlmostEqual(c.measurement('170 lbs','weight'),77.11)
        self.assertIsNone(c.measurement('185','height'))
        self.assertIsNone(c.measurement('185 kg','height'))
    def test_later_retrieval_does_not_leak_into_historical_prediction(self):
        r=row(observed=(T+timedelta(hours=1)).isoformat())
        self.assertFalse(c.eligible(r,T))
        r['published_at']=(T-timedelta(days=3)).isoformat()
        self.assertFalse(c.eligible(r,T))
    def test_future_publication_is_excluded(self):
        self.assertFalse(c.eligible(row(published=(T+timedelta(seconds=1)).isoformat()),T))
    def test_window_boundaries_and_current_match_exclusion(self):
        rows=[row(days=7,event='seven'),row(days=14,event='fourteen'),row(days=14.01,event='old'),row(days=0,event='target')]
        p=c.participant_package(KEY,'Test Player',rows,T,'target','player')
        self.assertEqual(p['windows']['7']['matches_observed'],1)
        self.assertEqual(p['windows']['14']['matches_observed'],2)
    def test_missing_minutes_are_not_zero(self):
        p=c.participant_package(KEY,'Test Player',[row(value={'result':'win','match_duration_minutes':None})],T,'target','player')
        self.assertIsNone(p['windows']['7']['minutes_observed'])
        self.assertEqual(p['windows']['7']['matches_with_minutes'],0)
        self.assertIsNone(p['fatigue_estimate'])
    def test_confirmed_old_injury_remains_relevant(self):
        r=row('availability','status',{'status':'reported_unavailable'},days=30,published=(T-timedelta(days=30)).isoformat(),metadata={'confirmation_kind':'current_provider_state'})
        p=c.participant_package(KEY,'Test Player',[r],T,'target','player')
        self.assertEqual(p['availability_status'],'reported')
    def test_fetching_an_old_article_does_not_make_it_fresh(self):
        r=row('availability','status',{'status':'reported_unavailable'},days=30,published=(T-timedelta(days=30)).isoformat())
        p=c.participant_package(KEY,'Test Player',[r],T,'target','player')
        self.assertEqual(p['availability_status'],'unknown')
    def test_team_roster_side_is_not_guessed_to_be_preferred_foot(self):
        rows=c.profile_records('football','team:player:1','A',{'strSide':'Right','displayHeight':'185 cm'},'test','https://example.org')
        self.assertEqual([r['metric'] for r in rows],['height_cm'])
    def test_zero_sample_is_insufficient(self):
        p=c.participant_package(KEY,'Test Player',[],T,'target','player')
        self.assertEqual(p['windows']['14']['sample_status'],'insufficient_recent_evidence')
        self.assertIsNone(p['rest_hours'])
    def test_postponement_schedule_is_not_a_completed_appearance(self):
        e={'id':'id','starts_at':T.isoformat(),'metadata':{'league':'atp'}}
        state={'observed_at':T.isoformat(),'payload':{'status':{'type':{'completed':False}},'competitors':[{'id':'1','athlete':{'displayName':'A'}}]}}
        self.assertEqual([r['category'] for r in c.scoreboard_records(e,'tennis',state)],['fixture'])

class Routes(unittest.TestCase):
    def test_receipts_require_admin_authentication(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        app=FastAPI(); c.install({'app':app,'SUPABASE_KEY':None})
        body={'sport':'tennis','entity_key':KEY,'entity_name':'Test','category':'availability','metric':'status','value':{'status':'unavailable'},'effective_at':'2026-10-19T12:00:00Z','published_at':'2026-10-19T12:00:00Z','provider':'receipt','source_url':'https://example.org/report'}
        with patch.dict('os.environ',{'SDE_ADMIN_TOKEN':''}):
            self.assertEqual(TestClient(app).post('/context/evidence',json=body).status_code,403)

if __name__=='__main__': unittest.main()
