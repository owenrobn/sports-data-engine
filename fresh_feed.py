"""Bounded source collection with verified private-object persistence during DB recovery."""
import asyncio
import gzip
import hashlib
import json
import io
import uuid
from datetime import timedelta
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from context_feed import clock,date,get_json
from laboratory import competitions,uid,norm,page,table,esc

LEAGUES={'football':['eng.1','esp.1','ger.1','ita.1','ned.1','fra.1','uefa.champions'],'tennis':['atp','wta']}
STATE={s:{'packs':{},'last_poll_at':None,'errors':[],'archive':{'verified':False,'status':'pending'},'history_cursor':1} for s in LEAGUES}
ARCHIVE_LOCK=asyncio.Lock()
ARCHIVE_USAGE={'bytes':None,'checked_at':None}
CADENCE=120
MAX_ROWS=5000
MAX_OBJECT=8*1024*1024

def canonical(value): return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def normalize(event,comp,sport,league,observed):
    people=[]
    for p in comp.get('competitors') or []:
        who=p.get('athlete') or p.get('team') or {};name=who.get('displayName') or who.get('fullName')
        pid=who.get('id') or p.get('id')
        if not name or not pid or norm(name) in ('tbd','unknown','bye'): return None
        people.append({'source_id':str(pid),'name':name,'home_away':p.get('homeAway'),'score':p.get('score'),'sets':p.get('linescores'),'statistics':p.get('statistics') or [],'winner':p.get('winner'),'server':p.get('server') if 'server' in p else None})
    if len(people)!=2: return None
    pid=str(comp.get('id') or event.get('id') or '')
    if not pid: return None
    start=date(comp.get('date') or event.get('date'))
    if comp.get('timeValid') is False or event.get('timeValid') is False: start=None
    venue=comp.get('venue') or {}
    return {'id':uid('espn:'+pid),'provider_match_id':pid,'provider_event_id':str(event.get('id') or ''),'sport':sport,'league':league,'tournament':event.get('name') if sport=='tennis' else None,'name':' vs '.join(p['name'] for p in people),'starts_at':start.isoformat() if start else None,'source_date':comp.get('date') or event.get('date'),'status':comp.get('status') or event.get('status') or {},'participants':people,'venue':venue,'surface':comp.get('surface'),'indoor':comp.get('indoor') if 'indoor' in comp else venue.get('indoor'),'weather':comp.get('weather'),'observed_at':observed,'provider_event_time':None,'provider_latency_known':False,'source_url':f'https://site.api.espn.com/apis/site/v2/sports/{"soccer" if sport=="football" else "tennis"}/{league}/scoreboard'}

def decompress(data):
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as handle: raw=handle.read(MAX_OBJECT+1)
    if len(raw)>MAX_OBJECT: raise ValueError('Decompressed snapshot exceeds budget')
    return raw

def archive_write(snapshot,store):
    raw=canonical(snapshot)
    if len(raw)>MAX_OBJECT: raise ValueError('Fresh snapshot exceeds uncompressed budget')
    data=gzip.compress(raw,mtime=0);checksum=hashlib.sha256(data).hexdigest()
    observed=snapshot['archived_at'];sport=snapshot['sport']
    key=f'sde/fresh/v1/{sport}/{observed[:10]}/{observed.replace(":", "-")}-{checksum}.json.gz'
    manifest={'version':1,'object_key':key,'sha256':checksum,'logical_sha256':hashlib.sha256(raw).hexdigest(),'sport':sport,'archived_at':observed,'match_count':sum(len(p['matches']) for p in snapshot['packs'].values()),'format':'gzip-json','historical_database_export':False}
    receipt=canonical(manifest)
    if store.size()+len(data)+len(receipt)>store.limit: raise RuntimeError('Archive byte budget reached')
    store.write_once(key,data)
    readback=store.read(key)
    if hashlib.sha256(readback).hexdigest()!=checksum or decompress(readback)!=raw: raise ValueError('Archive read-back verification failed')
    store.write_once(key+'.manifest.json',receipt)
    if store.read(key+'.manifest.json')!=receipt: raise ValueError('Archive manifest verification failed')
    return {'verified':True,'status':'verified','archived_at':observed,'matches':manifest['match_count'],'sha256':checksum,'historical_database_export':False}

class BudgetedStore:
    """Avoid a bucket-wide listing on every two-minute write; count own bytes conservatively."""
    def __init__(self,store,usage): self.store=store;self.usage=usage;self.limit=store.limit
    def size(self):
        checked=self.usage['checked_at']
        if self.usage['bytes'] is None or checked is None or (clock()-checked).total_seconds()>=3600:
            self.usage.update(bytes=self.store.size(),checked_at=clock())
        return self.usage['bytes']
    def write_once(self,key,data):
        self.store.write_once(key,data)
        self.usage['bytes']=(self.usage['bytes'] or 0)+len(data)
    def read(self,key): return self.store.read(key)

def persist(snapshot):
    from archive_store import S3Store
    store=S3Store()
    try: return archive_write(snapshot,BudgetedStore(store,ARCHIVE_USAGE))
    finally: store.client.close()

def hydrate(sport):
    from archive_store import S3Store
    store=S3Store()
    try:
        for days in range(3):
            day=(clock()-timedelta(days=days)).date().isoformat();keys=[];pages=0
            for p in store.client.get_paginator('list_objects_v2').paginate(Bucket=store.bucket,Prefix=f'sde/fresh/v1/{sport}/{day}/'):
                keys.extend(x['Key'] for x in p.get('Contents',[]) if x['Key'].endswith('.json.gz.manifest.json'));pages+=1
                if pages>=4: raise RuntimeError('Fresh restore listing budget reached')
            if not keys: continue
            manifest_key=max(keys);key=manifest_key.removesuffix('.manifest.json')
            manifest=json.loads(store.read(manifest_key));data=store.read(key)
            if hashlib.sha256(data).hexdigest()!=manifest['sha256']: raise ValueError('Stored snapshot checksum mismatch')
            raw=decompress(data)
            if len(raw)>MAX_OBJECT or hashlib.sha256(raw).hexdigest()!=manifest['logical_sha256']: raise ValueError('Stored logical checksum mismatch')
            snapshot=json.loads(raw)
            if snapshot.get('sport')!=sport or snapshot.get('schema_version')!=1: raise ValueError('Stored snapshot identity mismatch')
            return snapshot
        return None
    finally: store.client.close()

def compact_packs(packs):
    owners={}
    for key,pack in packs.items():
        for m in pack['matches']:
            candidate=(m['observed_at'],key)
            if m['id'] not in owners or candidate>owners[m['id']]: owners[m['id']]=candidate
    return {key:{**pack,'source_matches_returned':pack.get('source_matches_returned',len(pack['matches'])),'matches':[m for m in pack['matches'] if owners[m['id']][1]==key]} for key,pack in packs.items()}

def score_display(match):
    if (match.get('status',{}).get('type') or {}).get('state')=='pre': return 'Not started'
    p=match.get('participants') or []
    if len(p)!=2: return 'Unknown'
    if all(x.get('score') is not None for x in p): return ' – '.join(str(x['score']) for x in p)
    sets=[x.get('sets') for x in p]
    if match.get('sport')=='tennis' and all(isinstance(x,list) and x for x in sets):
        games=[]
        for a,b in zip(*sets):
            av=a.get('displayValue') if a.get('displayValue') is not None else a.get('value')
            bv=b.get('displayValue') if b.get('displayValue') is not None else b.get('value')
            games.append(str(av if av is not None else '?')+' – '+str(bv if bv is not None else '?'))
        return ' | '.join(games)+' (set games)'
    return 'Unknown'

async def enrich(sport,state):
    """Keep bounded provider receipts without requiring the historical database."""
    receipts=state.setdefault('details',{})
    matches={m['id']:m for p in state['packs'].values() for m in p['matches']}
    state['details']={k:v for k,v in receipts.items() if k in matches}
    receipts=state['details'];now=clock()
    candidates=[]
    for m in matches.values():
        old=receipts.get(m['id']);checked=date(old.get('attempted_at')) if old else None
        interval=900 if (m['status'].get('type') or {}).get('state')=='in' else 21600
        if checked and (now-checked).total_seconds()<interval: continue
        candidates.append(m)
    candidates.sort(key=lambda m:(0 if (m['status'].get('type') or {}).get('state')=='in' else 1,abs(((date(m.get('starts_at')) or now)-now).total_seconds())))
    for m in candidates[:4]:
        league=m['league'];pid=m['provider_match_id'];parent=m.get('provider_event_id')
        if sport=='tennis' and not parent: continue
        url=(f'https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/summary' if sport=='football' else f'https://sports.core.api.espn.com/v2/sports/tennis/leagues/{league}/events/{parent}/competitions/{pid}')
        attempted=clock().isoformat()
        try:
            raw=await get_json(url,{'event':pid} if sport=='football' else None)
            keys=('header','gameInfo','boxscore','rosters','injuries','format') if sport=='football' else ('id','date','status','competitors','venue','surface','indoor','format','situation','type')
            payload={k:raw[k] for k in keys if k in raw}
            if not payload: raise ValueError('No recognized detail fields')
            receipt={'source_url':url,'source_parameters':{'event':pid} if sport=='football' else {},'observed_at':clock().isoformat(),'attempted_at':attempted,'source_updated_at':None,'independent_fixture_verification':False,'payload':payload}
            if len(canonical(receipt))>128*1024: raise ValueError('Detail receipt exceeds budget')
            receipts[m['id']]=receipt
            while len(canonical(receipts))>1024*1024:
                oldest=min(receipts,key=lambda k:receipts[k].get('observed_at') or receipts[k].get('attempted_at',''))
                del receipts[oldest]
        except Exception as ex:
            receipts[m['id']]={**receipts.get(m['id'],{}),'attempted_at':attempted,'last_error_class':type(ex).__name__}


async def enrich_profiles(sport,state):
    from context_feed import profile_records
    profiles=state.setdefault('profiles',{});targets={};now=clock()
    matches=[m for pack in state['packs'].values() for m in pack['matches']]
    matches.sort(key=lambda m:abs(((date(m.get('starts_at')) or now)-now).total_seconds()))
    for m in matches:
        for p in m.get('participants',[]): targets.setdefault(m['league']+':'+p['source_id'],(m,p))
    state['profiles']={k:v for k,v in profiles.items() if k in targets};profiles=state['profiles']
    attempts=0
    for key,(m,p) in targets.items():
        old=profiles.get(key,{});checked=date(old.get('attempted_at'))
        if checked and (now-checked).total_seconds()<21600: continue
        if attempts>=4: break
        attempts+=1;league=m['league'];pid=p['source_id'];attempted=clock().isoformat()
        url=(f'https://site.web.api.espn.com/apis/common/v3/sports/tennis/{league}/athletes/{pid}' if sport=='tennis' else f'https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/teams/{pid}/roster')
        try:
            raw=await get_json(url);observed=clock().isoformat();rows=[]
            if sport=='tennis':
                athlete=raw.get('athlete') or raw
                if str(athlete.get('id'))!=pid or norm(athlete.get('displayName') or athlete.get('fullName') or '')!=norm(p['name']): raise ValueError('Profile identity mismatch')
                rows=profile_records(sport,key,p['name'],athlete,'espn',url,observed)
            else:
                team=raw.get('team') or {}
                if str(team.get('id'))!=pid: raise ValueError('Roster team identity unverified')
                def athletes(nodes):
                    for node in nodes:
                        children=node.get('items') or node.get('athletes')
                        if isinstance(children,list): yield from athletes(children)
                        else: yield node.get('athlete') or node
                for athlete in athletes(raw.get('athletes') or []):
                    if athlete.get('id') and (athlete.get('displayName') or athlete.get('fullName')):
                        rows.extend(profile_records(sport,'espn:football:player:'+str(athlete['id']),athlete.get('displayName') or athlete['fullName'],athlete,'espn',url,observed))
            receipt={'source_url':url,'observed_at':observed,'attempted_at':attempted,'source_updated_at':None,'records':rows,'status':'fields_observed' if rows else 'fields_not_supplied','fitness_assessment':False}
            if len(canonical(receipt))>64*1024: raise ValueError('Profile receipt exceeds budget')
            profiles[key]=receipt
            while len(canonical(profiles))>512*1024:
                del profiles[min(profiles,key=lambda k:profiles[k].get('observed_at') or profiles[k].get('attempted_at',''))]
        except Exception as ex: profiles[key]={**old,'attempted_at':attempted,'last_error_class':type(ex).__name__}


async def poll(sport):
    state=STATE[sport];today=clock().date();dates=[today,today+timedelta(days=1),today-timedelta(days=state['history_cursor'])]
    errors=[];sem=asyncio.Semaphore(3)
    async def one(league,day):
        key=league+':'+day.isoformat();path='soccer' if sport=='football' else 'tennis'
        source=f'https://site.api.espn.com/apis/site/v2/sports/{path}/{league}/scoreboard'
        try:
            async with sem: payload=await get_json(source,{'dates':day.strftime('%Y%m%d'),'limit':1000})
            observed=clock().isoformat();matches=[]
            for e in payload.get('events') or []:
                for comp in competitions(e):
                    match=normalize(e,comp,sport,league,observed)
                    if match: matches.append(match)
            state['packs'][key]={'league':league,'requested_date':day.isoformat(),'source_url':source,'observed_at':observed,'matches':matches}
        except Exception as ex:
            import httpx
            errors.append({'league':league,'date':day.isoformat(),'error_class':type(ex).__name__,'http_status':ex.response.status_code if isinstance(ex,httpx.HTTPStatusError) else None})
    await asyncio.gather(*(one(league,day) for day in dates for league in LEAGUES[sport]))
    state['last_poll_at']=clock().isoformat();state['errors']=errors;state['history_cursor']=state['history_cursor']%14+1
    low=(today-timedelta(days=14)).isoformat();high=(today+timedelta(days=2)).isoformat()
    state['packs']={k:p for k,p in state['packs'].items() if low<=p['requested_date']<=high}
    state['packs']=compact_packs(state['packs'])
    count=sum(len(p['matches']) for p in state['packs'].values())
    if count>MAX_ROWS:
        state['archive']={'verified':False,'status':'row_budget_reached'};return
    await enrich(sport,state)
    await enrich_profiles(sport,state)
    snapshot={'schema_version':1,'details':state.get('details',{}),'profiles':state.get('profiles',{}),'sport':sport,'archived_at':clock().isoformat(),'last_poll_at':state['last_poll_at'],'history_cursor':state['history_cursor'],'packs':state['packs']}
    async with ARCHIVE_LOCK:
        try: state['archive']=await asyncio.to_thread(persist,snapshot)
        except Exception as ex: state['archive']={'verified':False,'status':'persistence_failed','error_class':type(ex).__name__}


def restore_state(sport,snapshot):
    cursor=snapshot.get('history_cursor',1)
    if type(cursor) is not int or not 1<=cursor<=14: raise ValueError('Invalid saved history cursor')
    STATE[sport]['profiles']=snapshot.get('profiles',{});STATE[sport]['details']=snapshot.get('details',{});STATE[sport]['packs']=snapshot['packs'];STATE[sport]['history_cursor']=cursor
    STATE[sport]['last_poll_at']=snapshot.get('last_poll_at')
    STATE[sport]['archive']={'verified':True,'status':'restored_verified_snapshot','archived_at':snapshot['archived_at']}

def output(sport):
    if sport not in LEAGUES: raise HTTPException(422,'Choose football or tennis')
    state=STATE[sport];now=clock();matches={}
    for pack in state['packs'].values():
        for r in pack['matches']:
            if r['id'] not in matches or r['observed_at']>matches[r['id']]['observed_at']: matches[r['id']]=r
    rows=[]
    for r in matches.values():
        t=date(r.get('observed_at'));age=(now-t).total_seconds() if t else None
        rows.append({**r,'detail_receipt':state.get('details',{}).get(r['id']),'retrieval_age_seconds':round(age,1) if age is not None else None,'retrieval_fresh':age is not None and 0<=age<=180})
    rows.sort(key=lambda r:(0 if (r['status'].get('type') or {}).get('state')=='in' else 1,abs(((date(r.get('starts_at')) or now)-now).total_seconds())))
    return {'sport':sport,'generated_at':now.isoformat(),'collection_mode':'private_archive_during_database_recovery','engine_ready':False,'database_collection_paused':True,'last_poll_at':state['last_poll_at'],'matches_observed':len(rows),'profiles':state.get('profiles',{}),'upstream_errors':state['errors'],'archive':state['archive'],'matches':rows,'coverage':{'selected_leagues':LEAGUES[sport],'history_days_fetched':len({p['requested_date'] for p in state['packs'].values() if p['requested_date']<now.date().isoformat()}),'target_history_days':14,'complete_schedule_verified':False},'limitations':['This independent lane does not restore the full database or its historical archive.','Recent retrieval does not establish provider latency.','Profiles, availability, minutes, market benchmark and full PRD inputs remain incomplete.','Free hosting can sleep. Memory-only observations are labelled when private persistence fails.']}

def match_package(event_id):
    for sport in LEAGUES:
        data=output(sport);target=next((m for m in data['matches'] if m['id']==str(event_id)),None)
        if not target: continue
        start=date(target.get('starts_at'))
        if start is None: raise HTTPException(422,'Verified timezone-aware match start required for pre-match windows')
        cutoff=min(clock(),start);participants=[]
        for p in target['participants']:
            prior=[]
            for m in data['matches']:
                start=date(m.get('starts_at'));observed=date(m.get('observed_at'));completed=(m['status'].get('type') or {}).get('completed') is True
                if m['id']==target['id'] or not start or not observed or observed>cutoff or not cutoff-timedelta(days=14)<=start<cutoff or not completed: continue
                person=next((x for x in m['participants'] if x['source_id']==p['source_id']),None)
                if person: prior.append({'event_id':m['id'],'name':m['name'],'starts_at':m['starts_at'],'observed_at':m['observed_at'],'source_url':m['source_url'],'participant':person})
            windows={str(days):{'matches_observed':sum(date(m['starts_at'])>=cutoff-timedelta(days=days) for m in prior),'evidence':[m for m in prior if date(m['starts_at'])>=cutoff-timedelta(days=days)],'minutes':None,'schedule_complete':False} for days in (7,14)}
            participants.append({'name':p['name'],'source_id':p['source_id'],'windows':windows,'profiles':data.get('profiles',{}).get(target['league']+':'+p['source_id']) if date(data.get('profiles',{}).get(target['league']+':'+p['source_id'],{}).get('observed_at')) and date(data['profiles'][target['league']+':'+p['source_id']]['observed_at'])<=cutoff else None,'availability':None,'rest_hours':None})
        return {'match':{k:v for k,v in target.items() if k!='detail_receipt'},'detail_receipt':target.get('detail_receipt') if date((target.get('detail_receipt') or {}).get('observed_at')) and date(target['detail_receipt']['observed_at'])<=cutoff else None,'pre_match_cutoff':cutoff.isoformat(),'participants':participants,'engine_ready':False,'source_coverage':data['coverage'],'archive':data['archive'],'missing_inputs':['verified_profiles','fresh_availability','complete_minutes','independent_fixture_verification','complete_live_model_inputs','synchronized_market_benchmark','private_account_integration'],'storage_mode':data['collection_mode']}
    raise HTTPException(404,'Match not yet observed in the independent feed')

def install(app):
    @app.get('/data/fresh')
    async def data(sport:str='tennis'): return output(sport)
    @app.get('/data/fresh/match/{event_id}')
    async def detail(event_id:uuid.UUID): return match_package(event_id)
    @app.get('/fresh/players',response_class=HTMLResponse)
    async def players(sport:str='tennis'):
        data=output(sport);rows=[]
        for receipt in data['profiles'].values():
            for r in receipt.get('records',[]):
                rows.append([esc(r['entity_name']),esc(r['metric']),esc(r['value']),esc(r.get('unit')),esc(r['observed_at']),esc(r['source_url'])])
        body='<p><a href="/fresh?sport='+sport+'">Matches</a> · <a href="?sport=football">Football profiles</a> · <a href="?sport=tennis">Tennis profiles</a></p><p>Identity-checked source measurements. Source update time is unknown. These are not fitness or injury assessments. Missing fields remain unknown; collection is bounded and coverage is partial.</p>'
        body+=table(['Player','Field','Value','Unit','Retrieved','Source'],rows)
        return page(sport.title()+' · Player profile evidence',body)
    @app.get('/fresh',response_class=HTMLResponse)
    async def view(sport:str='tennis'):
        p=output(sport);a=p['archive'];body='<p><a href="?sport=football">Football</a> · <a href="?sport=tennis">Tennis</a> · <a href="/data/fresh?sport='+sport+'">Evidence JSON</a> · <a href="/dashboard">Recovery status</a></p><p>Database recovery is ongoing. This independent lane collects selected fixtures and statistics into the connected private archive. It is not full IMG-RAE readiness.</p>'
        body+='<p><a href="/fresh/players?sport='+sport+'">Collected player profiles</a></p>'
        body+='<p>Last scan: '+esc(p['last_poll_at'])+'. Archive: '+esc(a['status'])+'. Read-back verified: '+esc(a['verified'])+'. Prior dates fetched: '+esc(p['coverage']['history_days_fetched'])+'/14.</p>'
        body+=table(['Match','Source start (UTC)','State','Score','Retrieved','Fresh','Details','Evidence'],[[esc(m['name']),esc(m['starts_at']),esc((m['status'].get('type') or {}).get('description')),esc(score_display(m)),esc(m['observed_at']),esc(m['retrieval_fresh']),esc('Collected' if (m.get('detail_receipt') or {}).get('payload') else 'Pending'),('<a href="/fresh/match/'+m['id']+'">7/14-day input package</a>' if m['starts_at'] else 'Start time unverified')] for m in p['matches']])
        if p['upstream_errors']: body+='<p>Source failures: '+esc(json.dumps(p['upstream_errors']))+'</p>'
        return page(sport.title()+' · Fresh source evidence',body)
    @app.get('/fresh/match/{event_id}',response_class=HTMLResponse)
    async def match_view(event_id:uuid.UUID):
        p=match_package(event_id);body='<p><a href="/fresh?sport='+p['match']['sport']+'">Match list</a> · <a href="/data/fresh/match/'+str(event_id)+'">Evidence JSON</a></p><p>Engine ready: False. Missing inputs: '+esc(', '.join(p['missing_inputs']))+'</p>'
        body+=table(['Participant','7-day matches observed','14-day matches observed','Availability','Minutes'],[[esc(x['name']),x['windows']['7']['matches_observed'],x['windows']['14']['matches_observed'],'Unknown','Unknown'] for x in p['participants']])
        body+='<details><summary>Source evidence</summary><pre>'+esc(json.dumps(p,indent=2))+'</pre></details>'
        return page(p['match']['name']+' · Independent evidence package',body)
    @app.on_event('startup')
    async def startup():
        from runtime_policy import collection_paused
        if not collection_paused(): return
        async def lane(sport):
            try:
                snapshot=await asyncio.to_thread(hydrate,sport)
                if snapshot: restore_state(sport,snapshot)
            except Exception as ex: STATE[sport]['archive']={'verified':False,'status':'restore_unavailable','error_class':type(ex).__name__}
            while True:
                try: await poll(sport)
                except asyncio.CancelledError: raise
                except Exception as ex: STATE[sport]['errors']=[{'error_class':type(ex).__name__}]
                await asyncio.sleep(CADENCE)
        app.state.fresh_tasks=[asyncio.create_task(lane(s)) for s in LEAGUES]
    @app.on_event('shutdown')
    async def shutdown():
        tasks=getattr(app.state,'fresh_tasks',[])
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
