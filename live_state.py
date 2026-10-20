"""Timestamped current-match evidence, separate from pre-match lookback windows."""
import asyncio
import json
import uuid
from datetime import timedelta
from urllib.parse import urlencode
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from context_feed import clock,date,get_json

CTX={}

def snapshot(comp,observed,source):
    participants=[]
    for p in comp.get('competitors') or []:
        identity=p.get('athlete') or p.get('team') or {}
        participants.append({'provider_id':identity.get('id') or p.get('id'),'name':identity.get('displayName') or identity.get('fullName'),'home_away':p.get('homeAway'),'score':p.get('score'),'sets':p.get('linescores'),'statistics':p.get('statistics'),'server':p.get('server') if 'server' in p else None})
    # Preserve only explicit provider fields. Court surface is never inferred from venue name.
    return {'observed_at':observed,'source_url':source,'status':comp.get('status'),'competitors':participants,'venue':comp.get('venue'),'weather':comp.get('weather'),'surface':comp.get('surface'),'indoor':comp.get('indoor'),'situation':comp.get('situation'),'format':comp.get('format'),'time_precision':'retrieved_snapshot_not_provider_event_time'}

async def collect(sport):
    from laboratory import pages,put,competitions,uid
    path='soccer' if sport=='football' else 'tennis'
    mappings=await pages('provider_mappings',provider='eq.espn',entity_type='eq.event',order='id.asc')
    known={r['provider_entity_id']:r['canonical_id'] for r in mappings}
    leagues=['eng.1','esp.1','ger.1','ita.1','ned.1','fra.1','uefa.champions'] if sport=='football' else ['atp','wta']
    total=0
    for league in leagues:
        source=f'https://site.api.espn.com/apis/site/v2/sports/{path}/{league}/scoreboard'
        payload=await get_json(source,{'dates':clock().strftime('%Y%m%d'),'limit':1000})
        rows=[]
        for event in payload.get('events') or []:
            for comp in competitions(event):
                pid=str(comp.get('id') or event.get('id'))
                if pid not in known: continue
                comp={**comp,'status':comp.get('status') or event.get('status')}
                state=(comp.get('status') or {}).get('type') or {}
                start=date(comp.get('date') or event.get('date'))
                now=clock()
                if state.get('completed') is True: continue
                if state.get('state')!='in' and not (start and now-timedelta(hours=6)<=start<=now+timedelta(minutes=30)): continue
                observed=now.isoformat(); data=snapshot(comp,observed,source)
                # Each minute has a receipt even if the score did not change: retrieval freshness is explicit.
                rows.append({'event_id':known[pid],'provider':'espn','provider_event_id':uid('live:'+pid+':'+observed[:16]),'event_type':'live_snapshot','effective_at':None,'observed_at':observed,'payload':data})
        if rows: await put(sport+'_match_events',rows)
        total+=len(rows)
    return total

def interpret(row,decision):
    observed=date(row.get('observed_at'))
    if not observed or observed>decision: raise ValueError('Snapshot not available at decision time')
    payload=row.get('payload') or {}; status=payload.get('status') or {}
    age=(decision-observed).total_seconds()
    return {'observed_at':observed.isoformat(),'age_seconds':round(age,2),'retrieval_fresh':age<=120,'provider_event_time':row.get('effective_at'),'provider_latency_known':False,'status':status,'participants':payload.get('competitors') or [],'venue':payload.get('venue'),'weather':payload.get('weather'),'surface':payload.get('surface'),'indoor':payload.get('indoor'),'situation':payload.get('situation'),'source_url':payload.get('source_url'),'note':'A recent retrieval does not prove the provider updated its underlying data.'}

async def package(event_id,as_of=None):
    from laboratory import sid_for
    events=await CTX['db']('GET','events',query='?'+urlencode({'id':'eq.'+str(event_id),'limit':1})) or []
    if not events: raise HTTPException(404,'Match not found')
    e=events[0];sport='football' if str(e['sport_id'])==str(await sid_for('football')) else 'tennis'
    decision=date(as_of) if as_of else clock()
    if not decision or decision>clock(): raise HTTPException(422,'Use a timezone-aware decision time no later than now')
    rows=await CTX['db']('GET',sport+'_match_events',query='?'+urlencode({'event_id':'eq.'+str(event_id),'event_type':'eq.live_snapshot','observed_at':'lte.'+decision.isoformat(),'order':'observed_at.desc','limit':1})) or []
    state=interpret(rows[0],decision) if rows else None
    required=['clock','score','shots_on_target','dangerous_attacks','red_cards','xg'] if sport=='football' else ['point_game_set_score','server','serve_points_won_total','return_points_won_total','tiebreak_state']
    return {'event_id':str(event_id),'match':e['name'],'sport':sport,'decision_time':decision.isoformat(),'mode':'live_snapshot','engine_ready':False,'snapshot':state,'required_model_inputs':required,'model_input_completeness':'not_verified','missing_snapshot':not bool(state),'performance_context_url':'/data/context/'+str(event_id),'limitations':['Live observations are not inserted into the pre-match information window.','Missing statistics remain unknown; scoreboards do not guarantee full PRD coverage.','Polling runs while the service is awake; continuous low-latency service is not guaranteed.']}

def install(ctx):
    CTX.update(ctx);app=ctx['app']
    @app.get('/data/live/{event_id}')
    async def data(event_id:uuid.UUID,as_of:str|None=None): return await package(event_id,as_of)
    @app.get('/live/{event_id}',response_class=HTMLResponse)
    async def view(event_id:uuid.UUID):
        from laboratory import page,esc
        p=await package(event_id)
        return page(p['match']+' · Current match evidence','<p><a href="/context">Matches</a> · <a href="/data/live/'+str(event_id)+'">Live JSON</a></p><p>Snapshots refresh approximately every minute while the service is awake. Missing fields remain unknown. Provider latency is not verified.</p><pre>'+esc(json.dumps(p,indent=2))+'</pre>')
    @app.on_event('startup')
    async def startup():
        if not ctx['SUPABASE_KEY']: return
        from laboratory import tracked,BACKGROUND
        async def lane(sport):
            while True:
                await tracked('live_evidence',sport,lambda:collect(sport))
                await asyncio.sleep(60)
        for sport in ('football','tennis'): BACKGROUND.append(asyncio.create_task(lane(sport)))
