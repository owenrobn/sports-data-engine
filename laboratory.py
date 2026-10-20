"""Factual research views and bounded, restart-safe collectors."""
import asyncio
from bisect import bisect_right
import csv
import html
import io
import json
import re
import uuid
import unicodedata
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode, quote
import httpx
from fastapi import HTTPException
from fastapi.responses import HTMLResponse

NS=uuid.UUID('f6265896-7731-462d-898f-6506167a308b')
CTX={}
SPORT_IDS={}
MAPPINGS={}
INSTANCE=str(uuid.uuid4())
BACKGROUND=[]
async def sid_for(sport):
    if sport not in SPORT_IDS: SPORT_IDS[sport]=await CTX['sport_id'](sport)
    return SPORT_IDS[sport]
def stamp(): return datetime.now(timezone.utc).isoformat()
def uid(text): return str(uuid.uuid5(NS,text))
def arr(v):
    if isinstance(v,list): return v
    try: return json.loads(v or '[]')
    except (ValueError,TypeError): return []
def esc(v): return html.escape(str(v if v is not None else 'Unknown'))
def norm(v):
    text=unicodedata.normalize('NFKD',str(v)).encode('ascii','ignore').decode().lower()
    return ' '.join(re.findall(r'[a-z0-9]+',text))
def competitions(event):
    result=[]
    def walk(node):
        if isinstance(node,dict):
            if len(node.get('competitors') or [])==2: result.append(node); return
            for key in ('groupings','competitions','events'):
                for child in node.get(key) or []: walk(child)
    walk(event)
    return result
def dt(v):
    try: return datetime.fromisoformat(v.replace('Z','+00:00')).astimezone(timezone.utc)
    except (ValueError,TypeError,AttributeError): return None
async def db(*a,**kw): return await CTX['db'](*a,**kw)
async def put(table,rows,keys=None,merge=False):
    if not rows: return []
    return await db('POST',table,rows,('?'+urlencode({'on_conflict':keys}) if keys else ''),prefer='return=minimal,resolution='+('merge-duplicates' if merge else 'ignore-duplicates'))
async def get(table,**query): return await db('GET',table,query='?'+urlencode(query)) or []
async def pages(table,**query):
    result=[]
    for offset in range(0,100000,100):
        batch=await get(table,**{**query,'limit':100,'offset':offset})
        result.extend(batch)
        if len(batch)<100: return result
    raise RuntimeError('Pagination safety limit reached; query a narrower universe')
async def fetch(url,params=None):
    async with httpx.AsyncClient(timeout=45,follow_redirects=True,headers={'User-Agent':'SportsDataEngine/0.6 public research'}) as c:
        for attempt in range(3):
            r=await c.get(url,params=params)
            if r.status_code==429 or r.status_code>=500:
                await asyncio.sleep(2**attempt); continue
            r.raise_for_status(); return r.json()
        raise RuntimeError('Provider temporarily unavailable')
async def tracked(provider,sport,fn):
    rid=str(uuid.uuid4())
    try:
        await put('sde_provider_runs',{'id':rid,'provider':provider,'sport':sport,'details':{'instance':INSTANCE}})
        n=await asyncio.wait_for(fn(), timeout=600)
        await db('PATCH','sde_provider_runs',{'status':'completed','finished_at':stamp(),'records':n or 0},'?id=eq.'+rid)
        return n or 0
    except asyncio.CancelledError: raise
    except Exception as ex:
        # Persist only error classes/status, never request URLs, headers or keys.
        msg=type(ex).__name__
        if isinstance(ex,httpx.HTTPStatusError):
            msg+=' HTTP '+str(ex.response.status_code)
            try:
                detail=ex.response.json(); msg+=' '+str(detail.get('code',''))
                if detail.get('code') in ('21000','57014','42P01','42501','23505','53100'): msg+=' '+str(detail.get('message',''))[:180]
            except (ValueError,AttributeError): pass
        try:
            await db('PATCH','sde_provider_runs',{'status':'failed','finished_at':stamp(),'error':msg},'?id=eq.'+rid)
        except Exception:
            pass  # A database outage must not kill the sport's collector lane.
        CTX['emit']('error','Collector failed',provider=provider,sport=sport,error=msg)
async def universe(provider,sport,key,expected,imported,description):
    await put('sde_coverage_universes',{'provider':provider,'sport':sport,'universe_key':key,'expected_matches':expected,'imported_matches':imported,'description':description,'observed_at':stamp()},'provider,sport,universe_key',True)
async def canonical(provider,pid,sport,name,start,metadata,status='scheduled'):
    if provider not in MAPPINGS:
        known=await pages('provider_mappings',provider='eq.'+provider,entity_type='eq.event',order='id.asc')
        MAPPINGS[provider]={m['provider_entity_id']:m['canonical_id'] for m in known}
    existing=MAPPINGS[provider].get(pid)
    eid=existing or uid(provider+':'+pid)
    sid=await sid_for(sport)
    await put('events',{'id':eid,'sport_id':sid,'name':name,'starts_at':start,'status':status,'metadata':{'source':provider,'provider_match_id':pid,**metadata},'updated_at':stamp()},'id',True)
    if not existing:
        await put('provider_mappings',{'provider':provider,'entity_type':'event','provider_entity_id':pid,'canonical_id':eid},'provider,entity_type,provider_entity_id')
        MAPPINGS[provider][pid]=eid
    return eid

async def espn(sport):
    leagues=['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'] if sport=='football' else ['atp','wta']
    path='soccer' if sport=='football' else 'tennis'; total=0; seen=set()
    sid=await sid_for(sport)
    known=await pages('provider_mappings',provider='eq.espn',entity_type='eq.event',order='id.asc')
    known={m['provider_entity_id']:m['canonical_id'] for m in known}
    dates=[(datetime.now(timezone.utc)+timedelta(days=days)).strftime('%Y%m%d') for days in range(2,-15,-1)]
    market_dates=await pages('polymarket_market_catalog',select='market_id,start_time',sport='eq.'+sport,order='market_id.asc')
    history=sorted({dt(m.get('start_time')).strftime('%Y%m%d') for m in market_dates if dt(m.get('start_time')) and dt(m['start_time'])<=datetime.now(timezone.utc)},reverse=True)
    history=[d for d in history if d not in dates]
    checkpoints=await get('backfill_checkpoints',provider='eq.espn',stream='eq.'+sport,cursor_key='eq.catalog_dates',limit=1)
    offset=int(checkpoints[0].get('cursor_value') or 0) if checkpoints else 0
    if offset>=len(history): offset=0
    dates+=history[offset:offset+2]
    for date in dates:
        for league in leagues:
            payload=await fetch(f'https://site.api.espn.com/apis/site/v2/sports/{path}/{league}/scoreboard',{'dates':date,'limit':1000})
            events=payload.get('events') or []; imported=0; event_rows=[]; mapping_rows=[]; state_rows=[]; expected=0
            for event in events:
                for comp in competitions(event):
                    scheduled=comp.get('date') or event.get('date')
                    if dt(scheduled) and dt(scheduled).strftime('%Y%m%d')!=date: continue
                    players=comp.get('competitors') or []
                    if len(players)!=2: continue
                    def label(p): return (p.get('team') or p.get('athlete') or {}).get('displayName') or p.get('displayName')
                    names=[label(p) for p in players]
                    if not all(names) or any(norm(n) in ('tbd','unknown','bye') for n in names): continue
                    expected+=1
                    pid=str(comp.get('id') or event['id']); start=scheduled
                    if pid in seen: continue
                    seen.add(pid)
                    if comp.get('timeValid') is False or event.get('timeValid') is False: start=None
                    if dt(start) is None: start=None
                    competition=(payload.get('leagues') or [{}])[0].get('name') or league
                    state=comp.get('status') or event.get('status') or {}
                    eid=known.get(pid) or uid('espn:'+pid)
                    event_rows.append({'id':eid,'sport_id':sid,'name':' vs '.join(names),'starts_at':start,'status':(state.get('type') or {}).get('name','scheduled'),'updated_at':stamp(),'metadata':{'source':'espn','provider_match_id':pid,'competition':competition,'tournament':event.get('name') if sport=='tennis' else None,'participants':names,'league':league,'provider_event_id':str(event['id']),'source_url':f'https://site.api.espn.com/apis/site/v2/sports/{path}/{league}/scoreboard','observed_at':stamp(),'source_date':scheduled[:10] if scheduled else None,'time_precision':'provider_scheduled' if start else 'date_only','status':state}})
                    mapping_rows.append({'provider':'espn','entity_type':'event','provider_entity_id':pid,'canonical_id':eid})
                    # A scoreboard is an observation, never a backdated point/event.
                    row={'event_id':eid,'provider':'espn','provider_event_id':uid(pid+':'+json.dumps({'status':state,'scores':[p.get('score') for p in players],'sets':[p.get('linescores') for p in players]},sort_keys=True)),'event_type':'score_observation','effective_at':None,'observed_at':stamp(),'payload':{'status':state,'competitors':players,'source_url':f'https://site.api.espn.com/apis/site/v2/sports/{path}/{league}/scoreboard'}}
                    if sport=='football':
                        row.update(home_score=None,away_score=None)
                        for p in players:
                            if p.get('homeAway') not in ('home','away'): continue
                            k=p['homeAway']+'_score'
                            try: row[k]=int(p.get('score'))
                            except (ValueError,TypeError): pass
                    state_rows.append(row)
                    imported+=1; total+=1
            for j in range(0,len(event_rows),100):
                await put('events',event_rows[j:j+100],'id',True)
                await put('provider_mappings',mapping_rows[j:j+100],'provider,entity_type,provider_entity_id')
                await put(sport+'_match_events',state_rows[j:j+100])
            await universe('espn',sport,league+':'+date,expected,imported,'ESPN '+league+' named matches on '+date+'; excludes unlisted matches and unresolved brackets')
    await put('backfill_checkpoints',{'provider':'espn','stream':sport,'cursor_key':'catalog_dates','cursor_value':str(offset+2),'status':'running','updated_at':stamp()},'provider,stream,cursor_key',True)
    return total

async def catalog(sport):
    # Search pages supplement paginated active/closed feeds; both sports receive the same budget.
    total=0
    items=[]; catalog_rows=[]; snapshot_rows=[]; checkpoints=[]
    sid=await sid_for(sport)
    recent=await get('events',select='id,name,metadata,starts_at',sport_id='eq.'+sid,starts_at='gte.'+(datetime.now(timezone.utc)-timedelta(days=7)).isoformat(),order='starts_at.desc',limit=20)
    queries=[('tennis' if sport=='tennis' else 'soccer')]+[e['name'] for e in recent if (e.get('metadata') or {}).get('source')=='espn']
    for query in queries:
        try:
            p=await fetch('https://gamma-api.polymarket.com/public-search',{'q':query,'limit_per_type':50,'events_status':'all'})
            items.extend(p.get('events',[]))
        except (httpx.HTTPError,RuntimeError):
            CTX['emit']('warning','Supplementary market search unavailable',sport=sport)
    for closed in ('false','true'):
        cp=await get('backfill_checkpoints',provider='eq.polymarket',stream='eq.'+sport,cursor_key='eq.'+closed,limit=1)
        offset=int(cp[0].get('cursor_value') or 0) if cp else 0
        batch=await fetch('https://gamma-api.polymarket.com/events',{'closed':closed,'limit':100,'offset':offset,'order':'id','ascending':'false'})
        items.extend(batch)
        # Existing table has provider/stream/cursor_key uniqueness checked before writes.
        checkpoints.append((cp,closed,offset+100 if batch else 0))
    archived={r['provider_resource_id'] for r in await pages('raw_ingestions',select='provider_resource_id',provider='eq.gamma_event',order='id.asc')}
    seen=set()
    for event in items:
        resource=str(event.get('id') or '')
        if not resource or resource in seen: continue
        seen.add(resource)
        if CTX['classify_sport'](event)!=sport:
            title=' '+norm(event.get('title',''))+' '
            exact=any(all(' '+norm(n)+' ' in title for n in (e.get('metadata') or {}).get('participants',[])) and len((e.get('metadata') or {}).get('participants',[]))==2 for e in recent)
            if not exact: continue
        if resource not in archived:
            await CTX['raw_ingest']('gamma_event',resource,event)
            archived.add(resource)
        for m in event.get('markets') or []:
            mid=str(m.get('id') or '')
            if not mid: continue
            tokens=arr(m.get('clobTokenIds')); outcomes=arr(m.get('outcomes'))
            start=m.get('gameStartTime') or event.get('startTime')
            catalog_rows.append({'market_id':mid,'polymarket_event_id':str(event['id']),'condition_id':m.get('conditionId'),'question':m.get('question'),'slug':m.get('slug'),'sport':sport,'start_time':start,'end_time':m.get('endDate'),'closed':bool(m.get('closed')),'resolved':bool(m.get('resolved')),'observed_at':stamp(),'metadata':{'event_title':event.get('title'),'event_slug':event.get('slug'),'outcomes':outcomes,'clobTokenIds':tokens,'source':'gamma','source_url':'https://gamma-api.polymarket.com/events','start_time_field':'gameStartTime or event.startTime','market_type':m.get('sportsMarketType'),'event':{k:v for k,v in event.items() if k!='markets'}}})
            prices=arr(m.get('outcomePrices'))
            rows=[{'venue':'polymarket','market_id':mid,'condition_id':m.get('conditionId'),'token_id':str(t),'outcome':str(outcomes[i]) if i<len(outcomes) else None,'price':float(prices[i]) if i<len(prices) else None,'observed_at':stamp(),'payload':{'stream':'gamma_current'}} for i,t in enumerate(tokens)]
            snapshot_rows.extend(rows)
            total+=1
    unique={r['market_id']:r for r in catalog_rows}
    rows=list(unique.values())
    for j in range(0,len(rows),100): await put('polymarket_market_catalog',rows[j:j+100],'market_id',True)
    for j in range(0,len(snapshot_rows),250): await put('prediction_market_snapshots',snapshot_rows[j:j+250])
    for cp,closed,offset in checkpoints:
        if cp: await db('PATCH','backfill_checkpoints',{'cursor_value':str(offset),'updated_at':stamp()},'?id=eq.'+str(cp[0]['id']))
        else: await put('backfill_checkpoints',{'provider':'polymarket','stream':sport,'cursor_key':closed,'cursor_value':str(offset),'status':'running'})
    return total

def match_candidate(event,market):
    meta=event.get('metadata') or {}
    if meta.get('participant_names_incomplete'): return None
    names=meta.get('participants') or re.split(r'\s+(?:vs\.?|v)\s+',event.get('name',''),flags=re.I)
    if len(names)!=2: return None
    if any(not norm(n) or norm(n) in ('tbd','unknown','bye') for n in names): return None
    title=(market.get('metadata') or {}).get('event_title') or market.get('question') or ''
    title=' '+norm(title)+' '
    exact=all(' '+norm(p)+' ' in title for p in names)
    if not exact:
        # Suggestions may tolerate surname/club-form differences, never acceptance.
        tokens=[set(norm(n).split())-{'fc','cf','afc','club'} for n in names]
        if not all(t and t.issubset(set(title.split())) for t in tokens): return None
    a,b=dt(event.get('starts_at')),dt(market.get('start_time'))
    distance=abs((a-b).total_seconds())/60 if a and b else None
    # Exact full participant pair + provider match time; no fuzzy auto-acceptance.
    return {'accepted':exact and distance is not None and distance<=180,'distance':distance,'reason':'exact participant pair and match-time agreement' if exact else 'club-form suggestion; review required'}
async def link(sport):
    sid=await sid_for(sport)
    events=await pages('events',select='id,name,starts_at,metadata',sport_id='eq.'+sid,order='id.asc')
    markets=await pages('polymarket_market_catalog',select='market_id,polymarket_event_id,condition_id,question,start_time,metadata->event_title,metadata->market_type',sport='eq.'+sport,order='market_id.asc')
    total=0
    for m in markets: m['metadata']={'event_title':m.get('event_title'),'market_type':m.get('market_type')}
    for e in events:
        if (e.get('metadata') or {}).get('source')=='polymarket': continue
        for m in markets:
            candidate=match_candidate(e,m)
            if candidate is None: continue
            accepted=candidate['accepted']; confidence=1 if accepted else .5
            await put('market_link_candidates',{'event_id':e['id'],'market_id':m['market_id'],'polymarket_event_id':m['polymarket_event_id'],'candidate_title':m['question'],'sport':sport,'event_start':e.get('starts_at'),'market_start':m.get('start_time'),'time_distance_minutes':candidate['distance'],'name_similarity':1,'confidence':confidence,'status':'accepted' if accepted else 'candidate','metadata':candidate},'event_id,market_id',True)
            if accepted:
                await put('market_match_links',{'event_id':e['id'],'market_id':m['market_id'],'condition_id':m.get('condition_id'),'market_question':m['question'],'market_type':(m.get('metadata') or {}).get('market_type'),'confidence':1,'method':'exact_pair_time_v2','metadata':{'sport':sport,**candidate}},'event_id,market_id',True)
                total+=1
    return total

async def market_history(sport):
    links=await pages('market_match_links',select='event_id,market_id,condition_id,metadata',order='id.asc'); total=0
    for l in links:
        if (l.get('metadata') or {}).get('sport')!=sport: continue
        markets=await get('polymarket_market_catalog',select='market_id,condition_id,metadata->outcomes,metadata->clobTokenIds',market_id='eq.'+l['market_id'],limit=1)
        if not markets: continue
        m=markets[0]; meta=m; outcomes=arr(meta.get('outcomes'))
        for i,t in enumerate(arr(meta.get('clobTokenIds'))):
            history=await fetch('https://clob.polymarket.com/prices-history',{'market':t,'interval':'max','fidelity':1})
            rows=[{'event_id':l['event_id'],'venue':'polymarket','market_id':m['market_id'],'condition_id':m.get('condition_id'),'token_id':str(t),'outcome':outcomes[i] if i<len(outcomes) else None,'price':p['p'],'effective_at':datetime.fromtimestamp(int(p['t']),timezone.utc).isoformat(),'observed_at':stamp(),'payload':{'stream':'clob_price_history','source':'clob','token_id':str(t)}} for p in history.get('history',[]) if p.get('t') is not None and p.get('p') is not None]
            for j in range(0,len(rows),250): await put('prediction_market_snapshots',rows[j:j+250])
            total+=len(rows)
            try: await CTX['polymarket_book_snapshot'](str(t),l['event_id'],m['market_id'])
            except httpx.HTTPStatusError as ex:
                if ex.response.status_code!=404: raise
        if m.get('condition_id'): await CTX['polymarket_trades'](m['condition_id'],l['event_id'])
    return total

async def timeline(sport):
    links=await pages('market_match_links',order='id.asc'); total=0
    for l in links:
        if (l.get('metadata') or {}).get('sport')!=sport: continue
        states=await pages(sport+'_match_events',event_id='eq.'+l['event_id'],order='id.asc')
        prices=await pages('prediction_market_snapshots',market_id='eq.'+l['market_id'],effective_at='not.is.null',order='effective_at.asc,id.asc')
        timed=sorted([x for x in states if dt(x.get('effective_at'))],key=lambda x:dt(x['effective_at']))
        times=[dt(x['effective_at']) for x in timed]
        rows=[]
        for p in prices:
            price_time=dt(p.get('effective_at'))
            if price_time is None: continue
            position=bisect_right(times,price_time)-1
            state=timed[position] if position>=0 else None
            rows.append({'event_id':l['event_id'],'sport':sport,'market_id':l['market_id'],'condition_id':l.get('condition_id'),'token_id':p.get('token_id'),'outcome':p.get('outcome'),'market_price':p.get('price'),'effective_at':p['effective_at'],'observed_at':stamp(),'source':'clob','state':state or {},'metadata':{'price_snapshot_id':p['id'],'state_alignment':'last known prior state' if state else 'no timestamped sport state','state_effective_at':state.get('effective_at') if state else None}})
        for j in range(0,len(rows),250): await put('event_market_timeline',rows[j:j+250])
        total+=len(rows)
    return total

def parse_points(pbp,p1,p2):
    """Decode only documented singles codes and explicit source boundaries."""
    if any(c not in 'SRADsrad;./' for c in pbp): raise ValueError('Unknown PBP code')
    players=[p1,p2]; server=0; first=0; sets=[0,0]; games=[0,0]; points=[0,0]
    set_no=1; game_no=1; point_no=0; result=[]
    tiebreak=False
    def score(n,other):
        if n>=3 and other>=3: return 'AD' if n>other else '40'
        return ['0','15','30','40'][min(n,3)]
    for code in pbp:
        if code=='/': server=1-server; tiebreak=True; continue
        if code in ';.':
            winner=0 if points[0]>points[1] else 1
            if points[0]==points[1]: raise ValueError('Invalid tied game boundary')
            games[winner]+=1; points=[0,0]; server=1-first; first=server; tiebreak=False; game_no+=1
            if code=='.':
                winner=0 if games[0]>games[1] else 1
                sets[winner]+=1; games=[0,0]; set_no+=1; game_no=1
            continue
        tiebreak=tiebreak or games==[6,6]
        receiver=1-server; winner=server if code.upper() in 'SA' else receiver
        bp=not tiebreak and points[receiver]>=3 and points[receiver]>points[server]
        point_no+=1
        result.append({'event_type':'point','point_number':point_no,'set_number':set_no,'game_number':game_no,'server_name':players[server],'receiver_name':players[receiver],'player1_sets':sets[0],'player2_sets':sets[1],'player1_games':games[0],'player2_games':games[1],'player1_points':str(points[0]) if tiebreak else score(points[0],points[1]),'player2_points':str(points[1]) if tiebreak else score(points[1],points[0]),'is_break_point':bp,'is_tiebreak':tiebreak,'payload':{'code':code,'point_winner':players[winner],'state_phase':'before_point','derivation':'documented singles PBP and explicit game/set/serve boundaries','time_precision':'sequence_only'}})
        points[winner]+=1
    return result

async def tennis_points():
    total=0
    for tour in ('ATP','WTA'):
        name=tour+'_Singles_pbpx.csv'; url='https://raw.githubusercontent.com/TennisVisuals/tennis-match-data/master/point-by-point/'+name
        async with httpx.AsyncClient(timeout=60) as client:
            r=await client.get(url); r.raise_for_status(); source=list(csv.DictReader(io.StringIO(r.text)))
        cp=await get('backfill_checkpoints',provider='eq.tennisvisuals_v2',stream='eq.tennis',cursor_key='eq.'+name,limit=1)
        offset=int(cp[0].get('cursor_value') or 0) if cp else 0
        end=min(offset+20,len(source))
        for m in source[offset:end]:
            p1,p2=m.get('server1'),m.get('server2')
            if not p1 or not p2 or not m.get('pbp'): continue
            pid=':'.join([tour,m.get('date',''),m.get('tournament',''),m.get('round',''),p1,p2])
            try: states=parse_points(m['pbp'],p1,p2)
            except ValueError as ex:
                await CTX['raw_ingest']('tennisvisuals_quarantine',pid,{'source_url':url,'row':m,'reason':str(ex)})
                CTX['emit']('warning','Malformed tennis sequence preserved for review',provider_match_id=pid,reason=str(ex))
                continue
            eid=await canonical('tennisvisuals_v2',pid,'tennis',p1+' vs '+p2,None,{'participants':[p1,p2],'tournament':m.get('tournament'),'source_date':m.get('date'),'time_precision':'date_only','source_local_start_time':m.get('start_time'),'surface':m.get('surface'),'round':m.get('round'),'score':m.get('score'),'tour':tour,'source_url':url,'source_attribution':'TennisVisuals tennis-match-data; research archive','discipline':'singles'},'historical')
            for s in states: s.update(event_id=eid,provider='tennisvisuals_v2',provider_event_id=uid(pid+':'+str(s['point_number'])),surface=m.get('surface'),effective_at=None,observed_at=stamp())
            for j in range(0,len(states),250): await put('tennis_match_events',states[j:j+250])
            total+=len(states)
        await universe('tennisvisuals_v2','tennis',name,len(source),end,'Documented '+tour+' singles rich PBP archive; date-only records cannot align to historical market timestamps')
        if cp: await db('PATCH','backfill_checkpoints',{'cursor_value':str(end),'updated_at':stamp()},'?id=eq.'+str(cp[0]['id']))
        else: await put('backfill_checkpoints',{'provider':'tennisvisuals_v2','stream':'tennis','cursor_key':name,'cursor_value':str(end),'status':'running'})
    return total

async def run_engine(ctx):
    CTX.update(ctx)
    async def repair():
        total=0
        for table in ('prediction_market_snapshots','prediction_market_trades','prediction_market_books','event_market_timeline'):
            for _ in range(10):
                n=(await db('POST','rpc/sde_repair_tick',{'t':table})).get('processed',0)
                if not n: break
                total+=n
                await asyncio.sleep(.05)
        return total
    async def repair_loop():
        while True:
            await tracked('identity_repair','all',repair)
            await asyncio.sleep(900)
    BACKGROUND.append(asyncio.create_task(repair_loop()))
    async def archives():
        while True:
            await asyncio.gather(tracked('statsbomb','football',ctx['ingest_statsbomb_open']),tracked('tennisvisuals_v2','tennis',tennis_points))
            await asyncio.sleep(900)
    BACKGROUND.append(asyncio.create_task(archives()))
    while True:
        # Separate lanes give both sports equal time even when one provider is slow.
        async def lane(sport):
            for provider,fn in [('espn',espn),('polymarket_catalog',catalog),('linker',link),('polymarket_history',market_history),('timeline',timeline)]:
                await tracked(provider,sport,lambda fn=fn:fn(sport))
        try: await asyncio.gather(lane('football'),lane('tennis'))
        except asyncio.CancelledError: raise
        except Exception: ctx['emit']('error','Engine pass failed; retry scheduled')
        await asyncio.sleep(900)

STYLE='body{font-family:system-ui;max-width:1300px;margin:auto;padding:24px;background:#07101f;color:#e8eefc}a{color:#9dc8ff}table{border-collapse:collapse;width:100%;margin:16px 0}th,td{text-align:left;border-bottom:1px solid #34445b;padding:10px;vertical-align:top}pre{white-space:pre-wrap;overflow-wrap:anywhere}section{border:1px solid #34445b;padding:16px;margin:16px 0;border-radius:10px}.scroll{overflow:auto}button,input{font:inherit}small{color:#bac6dc}svg{width:100%;height:200px}summary{cursor:pointer}'
def page(title,body): return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'+esc(title)+'</title><style>'+STYLE+'</style></head><body><nav><a href="/dashboard">Overview</a> · <a href="/research/football">Football</a> · <a href="/research/tennis">Tennis</a> · <a href="/research/health">Provider health</a> · <a href="/context">Recent physical evidence</a></nav><h1>'+esc(title)+'</h1>'+body+'</body></html>'
def table(headers,rows): return '<div class="scroll"><table><thead><tr>'+''.join('<th scope="col">'+esc(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+str(c)+'</td>' for c in r)+'</tr>' for r in rows)+'</tbody></table></div>'
def percent(n,d): return f'{100*n/d:.1f}%' if d else 'Unknown (no declared universe)'
async def coverage_html():
    cov=await get('sde_sport_coverage',order='sport.asc')
    rows=[]
    for x in cov:
        d=x['canonical_matches']
        rows.append([f'<a href="/research/{x["sport"]}">{esc(x["sport"])}</a>',d,percent(x['matches_with_states'],d),percent(x.get('matches_with_points',0),d) if x['sport']=='tennis' else 'N/A',percent(x['linked_matches'],d),percent(x['matches_with_prices'],d),percent(x.get('matches_with_trades',0),d),percent(x.get('matches_with_books',0),d),x['catalog_markets'],percent(x['imported_target_matches'] or 0,x['target_matches'])])
    return '<section><h2>Football and tennis coverage</h2><p>State presence includes score observations; it does not imply a complete play-by-play record. Percentages use ingested sporting matches. Market-only events and quarantined point states are excluded. Declared-universe ingestion uses the sum of named provider targets, which can overlap.</p>'+table(['Sport','Matches','State presence','Point sequences','Accepted links','Historical prices','Trades','Book snapshots','Catalog','Declared-universe ingestion'],rows)+'<p>ESPN scoreboards cover selected competitions and dates. Archive timestamps and historical order-book depth remain unknown where unavailable.</p></section>'

def install(ctx):
    CTX.update(ctx); app=ctx['app']
    @app.on_event('shutdown')
    async def stop():
        tasks=BACKGROUND+[getattr(app.state,'engine',None)]
        tasks=[t for t in tasks if t is not None]
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        if ctx['SUPABASE_KEY']:
            await db('PATCH','sde_provider_runs',{'status':'interrupted','finished_at':stamp()},'?status=eq.running&details->>instance=eq.'+INSTANCE)
    original=next(r.endpoint for r in app.routes if getattr(r,'path',None)=='/dashboard')
    async def dashboard():
        baseline=await original()
        try: extra=await coverage_html()
        except Exception: extra='<section>Coverage temporarily unavailable. <a href="/research/health">Provider health</a></section>'
        extra='<nav><a href="/context">Recent physical evidence</a> · <a href="/research/football">Football</a> · <a href="/research/tennis">Tennis</a> · <a href="/research/health">Provider health and errors</a></nav>'+extra
        return baseline.replace('<div class=grid id=metrics>',extra+'<div class=grid id=metrics>',1)
    for r in list(app.routes):
        if getattr(r,'path',None)=='/dashboard': app.routes.remove(r)
    app.add_api_route('/dashboard',dashboard,response_class=HTMLResponse)

    @app.get('/research/health',response_class=HTMLResponse)
    async def health():
        runs=await get('sde_provider_runs',order='started_at.desc',limit=100)
        return page('Provider health', '<p>Free Render service: scheduled collection runs only while the instance is awake. Running entries without a finish time may reflect an interrupted instance.</p>'+table(['Provider','Sport','Status','Started','Finished','Records','Error'],[[esc(r.get(k)) for k in ('provider','sport','status','started_at','finished_at','records','error')] for r in runs]))
    @app.get('/research/{sport}',response_class=HTMLResponse)
    async def browse(sport:str,competition:str='',offset:int=0):
        if sport not in ('football','tennis'): raise HTTPException(404)
        sid=await ctx['sport_id'](sport)
        events=await pages('events',sport_id='eq.'+sid,order='updated_at.desc,id.asc')
        events=[e for e in events if (e.get('metadata') or {}).get('source')!='polymarket']
        def comp(e): return e['metadata'].get('tournament') or e['metadata'].get('competition') or 'Unclassified'
        groups=sorted({comp(e) for e in events})
        if competition: events=[e for e in events if comp(e)==competition]
        body='<p>'+ ' · '.join('<a href="?'+esc(urlencode({'competition':g}))+'">'+esc(g)+'</a>' for g in groups)+'</p>'
        body+=table(['Match','Competition / tournament','Start / source date','Provider','Status'],[[f'<a href="/match/{e["id"]}">{esc(e["name"])}</a>',esc(comp(e)),esc(e.get('starts_at') or e['metadata'].get('source_date')),esc(e['metadata'].get('source')),esc(e.get('status'))] for e in events[max(0,offset):max(0,offset)+100]])
        if len(events)>offset+100: body+='<a href="?'+esc(urlencode({'competition':competition,'offset':offset+100}))+'">Next 100 matches</a>'
        return page(sport.title()+' · '+(competition or 'Competitions / tournaments'),body)
    @app.get('/match/{event_id}',response_class=HTMLResponse)
    async def match(event_id:uuid.UUID):
        eid=str(event_id); events=await get('events',id='eq.'+eid,limit=1)
        if not events: raise HTTPException(404)
        event=events[0]; sports=await get('sports',id='eq.'+event['sport_id'],limit=1); sport=sports[0]['slug']
        if sport not in ('football','tennis'): raise HTTPException(404)
        states=await pages(sport+'_match_events',event_id='eq.'+eid,order='id.asc')
        links=await get('market_match_links',event_id='eq.'+eid)
        candidates=await get('market_link_candidates',event_id='eq.'+eid)
        timeline_rows=await pages('event_market_timeline',event_id='eq.'+eid,order='effective_at.asc,id.asc')
        body='<section><h2>Match provenance</h2><pre>'+esc(json.dumps(event,indent=2))+'</pre></section>'
        fields=['event_type','period','minute','second','team_name','player_name','xg'] if sport=='football' else ['event_type','set_number','game_number','point_number','server_name','receiver_name','player1_sets','player2_sets','player1_games','player2_games','player1_points','player2_points','is_break_point','is_tiebreak']
        body+='<section><h2>Sport-state timeline</h2><p>Unknown fields are left unknown. Source order is preserved for untimed sequences; these cannot be synchronized to price timestamps.</p>'+table(fields+['effective_at','observed_at','provider / ID'],[[esc(s.get(k)) for k in fields+['effective_at','observed_at']]+[esc(s.get('provider'))+' / '+esc(s.get('provider_event_id'))] for s in states])+'</section>'
        body+='<section><h2>Market linking</h2>'+table(['Market','Status','Confidence','Evidence'],[[esc(c.get('candidate_title')),esc(c.get('status')),esc(c.get('confidence')),esc(json.dumps(c.get('metadata')))] for c in candidates])+'</section>'
        body+='<section><h2>Historical match replay</h2><p>Only timestamped sport states are aligned to source price times. An empty state means no timestamped sport state was available; a displayed state is the last known prior observation, with its own timestamp.</p>'
        streams={}
        for t in timeline_rows: streams.setdefault((t.get('market_id'),t.get('token_id'),t.get('outcome')),[]).append(t)
        for (mid,token,outcome),series in streams.items():
            series=[t for t in series if t.get('market_price') is not None]
            if not series: continue
            a,b=dt(series[0]['effective_at']),dt(series[-1]['effective_at']); span=max(1,(b-a).total_seconds())
            pts=' '.join(f'{10+980*(dt(t["effective_at"])-a).total_seconds()/span:.2f},{190-180*float(t["market_price"]):.2f}' for t in series)
            body+='<h3>'+esc(mid)+' · '+esc(outcome or token)+'</h3><svg viewBox="0 0 1000 200" role="img" aria-label="Historical outcome price from zero to one"><path d="M10 10V190H990" fill="none" stroke="#bac6dc"/><polyline points="'+pts+'" fill="none" stroke="#55e6a5" stroke-width="2"/></svg><small>'+esc(series[0]['effective_at'])+' → '+esc(series[-1]['effective_at'])+'</small>'
        body+='<label>Replay position <input id="replay" type="range" min="0" max="'+str(max(0,len(timeline_rows)-1))+'" value="0"></label><pre id="replay-state">Select a replay position; the full factual table is available below.</pre>'
        body+=table(['Source time','Market','Outcome / token','Price','Last known sport state / time'],[[esc(t['effective_at']),esc(t.get('market_id')),esc(t.get('outcome') or t.get('token_id')),esc(t.get('market_price')),esc(json.dumps(t.get('state') or {}))] for t in timeline_rows])+'</section>'
        availability=[]
        for l in links:
            trades=await get('prediction_market_trades',market_id='in.('+l['market_id']+','+str(l.get('condition_id') or '')+')',limit=1)
            books=await get('prediction_market_books',market_id='eq.'+l['market_id'],order='observed_at.desc',limit=1)
            availability.append([esc(l['market_id']),'Available' if trades else 'No stored trades','Snapshot observed '+esc(books[0]['observed_at']) if books else 'No stored book snapshots'])
        body+='<section><h2>Trades and order books</h2><p>Book snapshots describe their observation time only. Historical depth is not reconstructed.</p>'+table(['Market','Trades','Order-book availability'],availability)+'</section>'
        data=json.dumps(timeline_rows).replace('<','\\u003c')
        body+='<script type="application/json" id="replay-data">'+data+'</script><script>const rows=JSON.parse(document.getElementById("replay-data").textContent);document.getElementById("replay").addEventListener("input",e=>{document.getElementById("replay-state").textContent=JSON.stringify(rows[Number(e.target.value)]||{},null,2)});</script>'
        return page(event['name'],body)


