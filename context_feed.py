"""Rolling, as-of evidence for IMG-RAE / LEDGER. No inferred medical facts."""
import asyncio
import hashlib
import hmac
import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from fastapi import HTTPException, Header
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import httpx

CTX={}
NS=uuid.UUID('4ec4b3d7-4860-46d0-83d8-45850fab6fa1')
UTC=timezone.utc
TABLE='sde_context_evidence'

def clock(): return datetime.now(UTC)
def date(v):
    if not isinstance(v,str): return None
    try:
        d=datetime.fromisoformat(v.replace('Z','+00:00'))
        return d.astimezone(UTC) if d.tzinfo else None
    except ValueError: return None

def number(v):
    try:
        n=float(v)
        return n if math.isfinite(n) else None
    except (TypeError,ValueError): return None

def measurement(v,kind):
    """Convert only explicitly labelled source units; never guess."""
    s=str(v or '').lower().strip()
    if kind=='height':
        feet=re.fullmatch(r"(\d+)\s*['′]\s*(\d+(?:\.\d+)?)\s*[\"″]?",s)
        if feet: return round((float(feet[1])*12+float(feet[2]))*2.54,2)
    m=re.fullmatch(r'(\d+(?:\.\d+)?)\s*(cm|m|in|inches|kg|kgs|lb|lbs)',s)
    if not m: return None
    n=float(m[1]); unit=m[2]
    if kind=='height' and unit in ('cm','m','in','inches'):
        return round(n*({'cm':1,'m':100,'in':2.54,'inches':2.54}[unit]),2)
    if kind=='weight' and unit in ('kg','kgs','lb','lbs'):
        return round(n*(.45359237 if unit in ('lb','lbs') else 1),2)
    return None

def record(sport,key,name,category,metric,value,provider,url,effective=None,event_id=None,unit=None,observed=None,published=None,updated=None,metadata=None):
    observed=observed or clock().isoformat()
    identity=[sport,key,category,metric,value,provider,event_id,effective]
    # Stable match observations retain the first retrieval. Profiles/statuses retain hourly versions.
    if category in ('profile','availability','collection'): identity.append(observed[:13])
    rid=str(uuid.uuid5(NS,json.dumps(identity,sort_keys=True,default=str)))
    return dict(id=rid,sport=sport,entity_key=key,entity_name=name,category=category,metric=metric,value=value,provider=provider,source_url=url,effective_at=effective or observed,event_id=event_id,unit=unit,observed_at=observed,published_at=published,source_updated_at=updated,metadata=metadata or {})

async def save(rows):
    unique=list({r['id']:r for r in rows}.values())
    for i in range(0,len(unique),100):
        await CTX['db']('POST',TABLE,unique[i:i+100],'?on_conflict=id',prefer='return=minimal,resolution=ignore-duplicates')
    return len(unique)

async def read(**query):
    return await CTX['db']('GET',TABLE,query='?'+urlencode(query)) or []

async def all_rows(**query):
    result=[]
    for offset in range(0,50000,200):
        page=await read(**{**query,'limit':200,'offset':offset})
        result.extend(page)
        if len(page)<200: return result
    raise RuntimeError('Evidence query exceeds safety limit')

async def get_json(url,params=None,headers=None):
    # Caller records only the base URL. Never persist API-token query strings.
    async with httpx.AsyncClient(timeout=25,follow_redirects=True) as c:
        response=await c.get(url,params=params,headers=headers)
        response.raise_for_status()
        return response.json()

def scoreboard_records(event,sport,observation):
    """Retain identities, complete results, and supplied stats. No invented match minutes."""
    meta=event.get('metadata') or {}; payload=observation.get('payload') or {}
    players=payload.get('competitors') or []
    status=payload.get('status') or meta.get('status') or {}
    done=(status.get('type') or {}).get('completed') is True
    start=date(event.get('starts_at'))
    if not start: return []
    seen=observation.get('observed_at') or clock().isoformat()
    url=payload.get('source_url') or meta.get('source_url') or 'https://www.espn.com/'
    rows=[]
    for p in players:
        source=p.get('athlete') if sport=='tennis' else p.get('team')
        source=source or {}; name=source.get('displayName') or source.get('fullName')
        pid=source.get('id') or p.get('id')
        if not name or not pid: continue
        key=f'espn:{sport}:{pid}'
        basis=dict(sport=sport,key=key,name=name,provider='espn',url=url,event_id=event['id'],effective=start.isoformat(),observed=seen)
        rows.append(record(**basis,category='fixture',metric='participant',value={'provider_id':str(pid),'kind':'player' if sport=='tennis' else 'team','league':meta.get('league'),'home_away':p.get('homeAway')}))
        if not done: continue
        sets=p.get('linescores') or []
        value={'result':'win' if p.get('winner') is True else 'loss' if p.get('winner') is False else 'unknown','score':p.get('score'),'sets_played':len(sets) if sets else None,'minutes_played':None,'match_duration_minutes':None,'completed_observed_at':seen,'kind':'player' if sport=='tennis' else 'team'}
        rows.append(record(**basis,category='appearance',metric='completed_match',value=value,metadata={'time_precision':'provider_scheduled_start','coverage':'selected ESPN competitions; not a complete player schedule'}))
        for stat in p.get('statistics') or []:
            metric=stat.get('name') or stat.get('label')
            if not metric: continue
            rows.append(record(**basis,category='match_stat',metric=str(metric),value={'value':stat.get('value'),'display_value':stat.get('displayValue')},metadata={'source_stat':stat}))
    return rows

def profile_records(sport,key,name,athlete,provider,url,observed=None):
    rawheight=athlete.get('displayHeight') or athlete.get('strHeight')
    rawweight=athlete.get('displayWeight') or athlete.get('strWeight')
    birth=athlete.get('dateOfBirth') or athlete.get('dateBorn')
    hand=(athlete.get('hand') or athlete.get('handedness') or athlete.get('strSide')) if sport=='tennis' else (athlete.get('preferredFoot') or athlete.get('foot'))
    if isinstance(hand,dict): hand=hand.get('displayName') or hand.get('abbreviation')
    fields={'height_cm':(measurement(rawheight,'height'),'cm'),'weight_kg':(measurement(rawweight,'weight'),'kg'),'date_of_birth':(birth,None),('handedness' if sport=='tennis' else 'preferred_foot'):(hand,None)}
    rows=[]
    for metric,(value,unit) in fields.items():
        if value is None or value=='': continue
        rows.append(record(sport,key,name,'profile',metric,value,provider,url,unit=unit,observed=observed,metadata={'source_update_unknown':True,'raw_height':rawheight,'raw_weight':rawweight,'profile_is_not_a_fitness_assessment':True}))
    return rows

async def cached_results(sport):
    from laboratory import pages,sid_for
    sid=await sid_for(sport)
    low=(clock()-timedelta(days=14)).isoformat(); high=(clock()+timedelta(days=3)).isoformat()
    events=await pages('events',select='id,name,starts_at,metadata',sport_id='eq.'+sid,starts_at='gte.'+low,**{'and':'(starts_at.lte.'+high+')'},order='id.asc')
    events={e['id']:e for e in events if (e.get('metadata') or {}).get('source')=='espn'}
    states=await pages(sport+'_match_events',select='event_id,payload,observed_at',provider='eq.espn',observed_at='gte.'+low,order='id.asc')
    rows=[]
    for state in states:
        if state['event_id'] in events: rows.extend(scoreboard_records(events[state['event_id']],sport,state))
    return await save(rows)

async def profiles(sport):
    from laboratory import norm
    low=(clock()-timedelta(days=14)).isoformat()
    fixtures=await all_rows(sport='eq.'+sport,category='eq.fixture',effective_at='gte.'+low,order='observed_at.desc,id.asc')
    recent=await all_rows(sport='eq.'+sport,category='eq.collection',metric='eq.profile_lookup',observed_at='gte.'+(clock()-timedelta(hours=6)).isoformat(),order='id.asc')
    attempted={r['entity_key'] for r in recent}; targets={}
    now=clock()
    fixtures.sort(key=lambda r:(0 if date(r['effective_at']) and now<=date(r['effective_at'])<=now+timedelta(days=2) else 1,abs((date(r['effective_at'])-now).total_seconds()) if date(r['effective_at']) else float('inf')))
    for f in fixtures: targets.setdefault(f['entity_key'],f)
    total=0
    for key,f in list((k,v) for k,v in targets.items() if k not in attempted)[:8]:
        v=f['value']; name=f['entity_name']; rows=[]; url='https://www.thesportsdb.com/api/v1/json/'+os.getenv('THESPORTSDB_KEY','123')+'/searchplayers.php'
        if sport=='tennis':
            league=v.get('league') or 'atp'
            espn_url=f'https://site.web.api.espn.com/apis/common/v3/sports/tennis/{league}/athletes/{v["provider_id"]}'
            try:
                payload=await get_json(espn_url); athlete=payload.get('athlete') or payload
                if norm(athlete.get('displayName') or athlete.get('fullName') or '')==norm(name): rows=profile_records(sport,key,name,athlete,'espn',espn_url)
            except (httpx.HTTPError,ValueError): pass
            if not rows:
                try:
                    payload=await get_json(url,{'p':name}); matches=[p for p in payload.get('player') or [] if norm(p.get('strPlayer'))==norm(name) and norm(p.get('strSport'))=='tennis']
                    if len(matches)==1: rows=profile_records(sport,key,name,matches[0],'thesportsdb','https://www.thesportsdb.com/player/'+str(matches[0]['idPlayer']))
                except (httpx.HTTPError,ValueError): pass
        else:
            league=v.get('league') or 'eng.1'
            roster_url=f'https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/teams/{v["provider_id"]}/roster'
            try:
                payload=await get_json(roster_url)
                def athletes(nodes):
                    for node in nodes:
                        children=node.get('items') or node.get('athletes')
                        if isinstance(children,list): yield from athletes(children)
                        else: yield node.get('athlete') or node
                for athlete in athletes(payload.get('athletes') or []):
                    aid=athlete.get('id'); aname=athlete.get('displayName') or athlete.get('fullName')
                    if not aid or not aname: continue
                    playerkey=f'espn:football:player:{aid}'
                    pr=profile_records(sport,playerkey,aname,athlete,'espn',roster_url)
                    for r in pr: r['metadata']['team_key']=key
                    rows.extend(pr)
            except (httpx.HTTPError,ValueError): pass
        rows.append(record(sport,key,name,'collection','profile_lookup',{'status':'available' if rows else 'not_available','provider_limitations':'Profiles do not establish injury status'},'sde','https://sports-data-engine.onrender.com/context'))
        total+=await save(rows)
        await asyncio.sleep(2)  # Stay below the documented free profile-search rate limit.
    return total

def eligible(row,cutoff):
    observed=date(row.get('observed_at')); published=date(row.get('published_at'))
    updated=date(row.get('source_updated_at'))
    return bool(observed and observed<=cutoff and (not published or published<=cutoff) and (not updated or updated<=cutoff))

def participant_package(key,name,rows,cutoff,target_id,kind):
    rows=[r for r in rows if r['entity_key']==key and eligible(r,cutoff)]
    profiles={}; availability=[]
    for r in sorted(rows,key=lambda r:r['observed_at']):
        if r['category']=='profile': profiles[r['metric']]=r
        if r['category']=='availability': availability.append(r)
    fresh_availability=[]
    for r in availability:
        confirmation=date(r.get('source_updated_at')) or date(r.get('published_at'))
        if r['metadata'].get('confirmation_kind')=='current_provider_state': confirmation=date(r['observed_at'])
        if confirmation and cutoff-timedelta(hours=24)<=confirmation<=cutoff and not (isinstance(r['value'],dict) and r['value'].get('affected_player_unknown')): fresh_availability.append(r)
    appearances={}
    for r in rows:
        t=date(r.get('effective_at'))
        if r['category']=='appearance' and r.get('event_id')!=target_id and t and cutoff-timedelta(days=14)<=t<cutoff:
            # One match per provider event, regardless of repeated final scoreboard observations.
            appearances.setdefault(r.get('event_id') or r['id'],r)
    windows={}
    for days in (7,14):
        sample=[r for r in appearances.values() if date(r['effective_at'])>=cutoff-timedelta(days=days)]
        minutes=[number(r['value'].get('minutes_played') if kind=='player' and r['sport']=='football' else r['value'].get('match_duration_minutes')) for r in sample]
        known=[v for v in minutes if v is not None]
        windows[str(days)]={'window_start':(cutoff-timedelta(days=days)).isoformat(),'window_end_exclusive':cutoff.isoformat(),'matches_observed':len(sample),'wins_observed':sum(r['value'].get('result')=='win' for r in sample),'minutes_observed':round(sum(known),2) if known else None,'matches_with_minutes':len(known),'minutes_coverage':len(known)/len(sample) if sample else None,'sample_status':'available_partial_schedule' if sample else 'insufficient_recent_evidence','statistics_evidence':[r for r in rows if r['category']=='match_stat' and r.get('event_id')!=target_id and date(r.get('effective_at')) and cutoff-timedelta(days=days)<=date(r['effective_at'])<cutoff],'evidence':sample}
    latest=max(appearances.values(),key=lambda r:r['effective_at'],default=None)
    # Scheduled start is not actual finish. Do not turn it into recovery time.
    gaps=(cutoff-date(latest['effective_at'])).total_seconds()/3600 if latest else None
    missing=[]
    if not profiles and kind=='player': missing.append('verified_player_profile')
    if not fresh_availability: missing.append('fresh_availability_confirmation')
    if not windows['14']['matches_observed']: missing.append('recent_completed_match_evidence')
    if windows['7']['matches_with_minutes']!=windows['7']['matches_observed'] or not windows['7']['matches_observed']: missing.append('complete_7_day_minutes_for_fatigue')
    return {'entity_key':key,'name':name,'kind':kind,'profiles':profiles,'profile_source_updated_at_unknown':any(not r.get('source_updated_at') for r in profiles.values()),'availability':fresh_availability,'availability_status':'reported' if fresh_availability else 'unknown','windows':windows,'hours_since_last_scheduled_start':gaps,'rest_hours':None,'fatigue_estimate':None,'missing_required_inputs':missing}

async def package(event_id,as_of=None):
    events=await CTX['db']('GET','events',query='?'+urlencode({'id':'eq.'+str(event_id),'limit':1})) or []
    if not events: raise HTTPException(404,'Match not found')
    event=events[0]; start=date(event.get('starts_at'))
    if not start: raise HTTPException(422,'A verified timezone-aware match start is required')
    requested=date(as_of) if as_of else clock()
    if requested is None or requested>clock(): raise HTTPException(422,'as_of must be a timezone-aware time at or before now')
    cutoff=min(start,requested)
    fixtures=await all_rows(event_id='eq.'+str(event_id),category='eq.fixture',order='observed_at.asc,id.asc')
    entities={r['entity_key']:r for r in fixtures if eligible(r,cutoff)}
    participants=[]
    for key,r in entities.items():
        rows=await all_rows(entity_key='eq.'+key,observed_at='lte.'+cutoff.isoformat(),effective_at='gte.'+(cutoff-timedelta(days=14)).isoformat(),order='observed_at.asc,id.asc')
        # Stable profiles are not discarded merely because their measurements are older than 14 days.
        older=await read(entity_key='eq.'+key,category='eq.profile',observed_at='lte.'+cutoff.isoformat(),order='observed_at.desc',limit=100)
        availability=await all_rows(entity_key='eq.'+key,category='eq.availability',observed_at='gte.'+(cutoff-timedelta(days=14)).isoformat(),order='observed_at.asc,id.asc')
        person=participant_package(key,r['entity_name'],rows+older+availability,cutoff,str(event_id),r['value'].get('kind','player'))
        if person['kind']=='team':
            squad=await all_rows(category='eq.profile',**{'metadata->>team_key':'eq.'+key},observed_at='lte.'+cutoff.isoformat(),order='observed_at.asc,id.asc')
            players={}
            for entry in squad:
                if eligible(entry,cutoff): players.setdefault(entry['entity_key'],{'name':entry['entity_name'],'profile':{}})['profile'][entry['metric']]=entry
            person['squad_profiles']=players
            if players:
                squad_rows=await all_rows(entity_key='in.('+','.join(players)+')',observed_at='lte.'+cutoff.isoformat(),effective_at='gte.'+(cutoff-timedelta(days=14)).isoformat(),order='observed_at.asc,id.asc')
                for playerkey,info in players.items(): info['recent_context']=participant_package(playerkey,info['name'],squad_rows+list(info['profile'].values()),cutoff,str(event_id),'player')
            if not players: person['missing_required_inputs'].append('verified_squad_profiles')
            person['missing_required_inputs'].append('complete_squad_player_minutes')
        participants.append(person)
    from rae_requirements import contract
    from laboratory import sid_for
    sport='football' if str(event.get('sport_id'))==str(await sid_for('football')) else 'tennis'
    requirements=contract(sport)
    missing=sorted({m for p in participants for m in p['missing_required_inputs']})
    if len(participants)!=2: missing.append('two_verified_pre_match_participants')
    return {'schema_version':'1.0','event_id':str(event_id),'match':event['name'],'match_start':start.isoformat(),'decision_cutoff':cutoff.isoformat(),'generated_at':clock().isoformat(),'windows_days':[7,14],'maximum_performance_lookback_days':14,'future_information_excluded':True,'historical_reconstruction_policy':'Evidence retrieved later is excluded unless an earlier archived observation exists. No backdating.','context_ready':not missing,'engine_ready':False,'rae_requirements':requirements,'missing_required_inputs':missing+['full_rae_requirements_not_yet_satisfied'],'participants':participants,'provider_access':provider_status(),'limitations':['Selected source coverage is not a complete player schedule.','No observation means unknown, never zero workload or injury-free.','This feed does not compute win probabilities or validate the PRD coefficients.']}

def provider_status():
    return {'espn_public':{'configured':True,'scope':'selected fixtures, results, profiles and rosters where public endpoints respond; no guaranteed SLA'},'thesportsdb':{'configured':True,'scope':'profile fallback; field and search coverage vary'},'api_tennis':{'configured':bool(os.getenv('API_TENNIS_KEY')),'scope':'recent tennis fixtures and match statistics'},'sportmonks':{'configured':bool(os.getenv('SPORTMONKS_API_TOKEN')),'scope':'football fixtures, player profiles, minutes and current absence reports'},'tracking_data':{'configured':False,'scope':'sprint speed and running distance require a licensed tracking feed'}}

class Receipt(BaseModel):
    sport:str
    entity_key:str=Field(min_length=1,max_length=180)
    entity_name:str=Field(min_length=1,max_length=180)
    event_id:uuid.UUID|None=None
    category:str
    metric:str=Field(min_length=1,max_length=100)
    value:dict|float|str
    unit:str|None=None
    effective_at:datetime
    published_at:datetime
    source_updated_at:datetime|None=None
    provider:str=Field(min_length=1,max_length=100)
    source_url:str=Field(max_length=1500)

def install(ctx):
    CTX.update(ctx); app=ctx['app']
    @app.get('/data/context/{event_id}')
    async def data(event_id:uuid.UUID,as_of:str|None=None): return await package(event_id,as_of)
    @app.get('/context/providers')
    async def providers(): return provider_status()
    @app.post('/context/evidence')
    async def receipt(r:Receipt,x_sde_admin_token:str=Header(default='')):
        expected=os.getenv('SDE_ADMIN_TOKEN','')
        if not expected or not hmac.compare_digest(expected,x_sde_admin_token): raise HTTPException(403,'Administrator authentication required')
        if r.sport not in ('football','tennis') or r.category not in ('profile','availability','appearance','match_stat'): raise HTTPException(422,'Unsupported evidence category')
        if not r.source_url.startswith('https://') or '?' in r.source_url or '#' in r.source_url: raise HTTPException(422,'Use an HTTPS source URL without credentials or query parameters')
        if r.category in ('appearance','availability','match_stat') and not isinstance(r.value,dict): raise HTTPException(422,'Structured evidence requires an object value')
        for t in (r.effective_at,r.published_at,r.source_updated_at):
            if t is not None and (t.tzinfo is None or t>clock()): raise HTTPException(422,'Evidence timestamps must be timezone-aware and not in the future')
        row=record(r.sport,r.entity_key,r.entity_name,r.category,r.metric,r.value,r.provider,r.source_url,effective=r.effective_at.isoformat(),published=r.published_at.isoformat(),updated=r.source_updated_at.isoformat() if r.source_updated_at else None,event_id=str(r.event_id) if r.event_id else None,unit=r.unit,metadata={'ingestion':'authenticated_source_receipt'})
        await save([row]); return {'id':row['id'],'status':'stored','note':'Receipt retained with provenance; not independent validation of the assertion'}
    @app.get('/context/players',response_class=HTMLResponse)
    async def players(sport:str='tennis'):
        from laboratory import page,table,esc
        if sport not in ('football','tennis'): raise HTTPException(422,'Choose football or tennis')
        rows=await all_rows(sport='eq.'+sport,category='eq.profile',order='observed_at.asc,id.asc')
        people={}
        for r in rows:
            info=people.setdefault(r['entity_key'],{'name':r['entity_name'],'fields':{}})
            info['fields'][r['metric']]=r
        output=[]
        for info in sorted(people.values(),key=lambda p:p['name']):
            f=info['fields']; latest=max(f.values(),key=lambda r:r['observed_at'])
            output.append([esc(info['name'])]+[esc((f.get(k) or {}).get('value')) for k in ('height_cm','weight_kg','handedness' if sport=='tennis' else 'preferred_foot','date_of_birth')]+[esc(latest['provider']),esc(latest['observed_at']),esc(latest.get('source_updated_at'))])
        body='<p><a href="/context">Match packages</a> · <a href="?sport=football">Football profiles</a> · <a href="?sport=tennis">Tennis profiles</a></p><p>Latest collected profiles. Measurements are not live fitness assessments. Unknown source-update times remain unknown.</p>'
        return page(sport.title()+' · Collected player profiles',body+table(['Player','Height (cm)','Weight (kg)','Hand / preferred foot','Birth date','Source','Retrieved','Source updated'],output))
    @app.get('/context',response_class=HTMLResponse)
    async def overview(sport:str='tennis'):
        from laboratory import page,table,esc,sid_for,norm
        if sport not in ('football','tennis'): raise HTTPException(422,'Choose football or tennis')
        sid=await sid_for(sport)
        events=await CTX['db']('GET','events',query='?'+urlencode({'sport_id':'eq.'+sid,'starts_at':'gte.'+(clock()-timedelta(days=14)).isoformat(),'order':'starts_at.desc','limit':100})) or []
        events=[e for e in events if (e.get('metadata') or {}).get('source')!='polymarket' and not any(norm(n) in ('tbd','unknown','bye') for n in (e.get('metadata') or {}).get('participants',[])) and date(e.get('starts_at')) and date(e['starts_at'])<=clock()+timedelta(days=3)]
        events.sort(key=lambda e:(0 if date(e['starts_at'])>=clock() else 1,abs((date(e['starts_at'])-clock()).total_seconds())))
        body='<p><a href="?sport=football">Football</a> · <a href="?sport=tennis">Tennis</a> · <a href="/context/providers">Source access</a> · <a href="/context/players?sport='+sport+'">Collected player profiles</a></p><p>Recent form uses 7 and 14 days before the decision cutoff. Injuries require a recent confirmation. Missing information stays unknown. Past matches may lack archived pre-match observations.</p>'
        body+=table(['Match','Start','Evidence'],[[esc(e['name']),esc(e.get('starts_at')),f'<a href="/context/match/{e["id"]}">View input package</a> · <a href="/live/{e["id"]}">Current match state</a>'] for e in events])
        return page('IMG-RAE / LEDGER · Recent evidence',body)
    @app.get('/context/match/{event_id}',response_class=HTMLResponse)
    async def view(event_id:uuid.UUID):
        from laboratory import page,esc,table
        p=await package(event_id)
        body='<p><a href="/rae">Full PRD coverage</a> · <a href="/context">Match list</a> · <a href="/data/context/'+str(event_id)+'">JSON for engines</a></p><p>Decision cutoff: '+esc(p['decision_cutoff'])+'</p><p>Engine ready: '+str(p['engine_ready'])+'</p><p>Missing inputs: '+esc(', '.join(p['missing_required_inputs']) or 'None')+'</p>'
        for athlete in p['participants']:
            body+='<section><h2>'+esc(athlete['name'])+'</h2><p>Availability: '+esc(athlete['availability_status'])+'</p>'
            body+=table(['Profile field','Value','Source','Observed','Source updated'],[[esc(k),esc(json.dumps(r['value'])),esc(r['provider']),esc(r['observed_at']),esc(r.get('source_updated_at'))] for k,r in athlete['profiles'].items()])
            squad=athlete.get('squad_profiles') or {}
            if squad:
                body+=table(['Squad player','Height (cm)','Weight (kg)','7-day minutes observed','14-day minutes observed'],[[esc(info['name']),esc((info['profile'].get('height_cm') or {}).get('value')),esc((info['profile'].get('weight_kg') or {}).get('value')),esc(info['recent_context']['windows']['7']['minutes_observed']),esc(info['recent_context']['windows']['14']['minutes_observed'])] for info in squad.values()])
            body+=table(['Window','Matches observed','Minutes known','Minutes coverage'],[[str(d)+' days',str(w['matches_observed']),esc(w['minutes_observed']),esc(w['minutes_coverage'])] for d,w in athlete['windows'].items()])+'</section>'
        body+='<details><summary>Full provenance and exclusions</summary><pre>'+esc(json.dumps(p,indent=2))+'</pre></details>'
        return page(p['match']+' · Recent evidence',body)
    @app.on_event('startup')
    async def start():
        if not ctx['SUPABASE_KEY']: return
        from laboratory import tracked,BACKGROUND
        async def lane(sport):
            while True:
                await tracked('recent_evidence',sport,lambda:cached_results(sport))
                await tracked('physical_profiles',sport,lambda:profiles(sport))
                await asyncio.sleep(300)
        for sport in ('football','tennis'): BACKGROUND.append(asyncio.create_task(lane(sport)))
        async def paid_lane(provider,sport,fn):
            while True:
                await tracked(provider,sport,fn)
                await asyncio.sleep(900)
        if os.getenv('API_TENNIS_KEY'): BACKGROUND.append(asyncio.create_task(paid_lane('api_tennis_context','tennis',api_tennis)))
        if os.getenv('SPORTMONKS_API_TOKEN'): BACKGROUND.append(asyncio.create_task(paid_lane('sportmonks_context','football',sportmonks)))

async def api_tennis():
    """Optional authenticated fixture/statistics adapter. Source IDs are kept distinct."""
    from laboratory import canonical
    base='https://api.api-tennis.com/tennis/'
    params={'method':'get_fixtures','APIkey':os.environ['API_TENNIS_KEY'],'date_start':(clock()-timedelta(days=14)).date().isoformat(),'date_stop':(clock()+timedelta(days=2)).date().isoformat(),'timezone':'UTC'}
    payload=await get_json(base,params)
    if payload.get('success')!=1: raise RuntimeError('API Tennis did not return success; check access and coverage')
    rows=[]
    for fixture in payload.get('result') or []:
        if 'doubles' in str(fixture.get('event_type_type','')).lower(): continue
        start=date(str(fixture.get('event_date',''))+'T'+str(fixture.get('event_time',''))+':00+00:00')
        names=[fixture.get('event_first_player'),fixture.get('event_second_player')]
        ids=[fixture.get('first_player_key'),fixture.get('second_player_key')]
        if not start or not all(names) or not all(ids): continue
        pid=str(fixture['event_key']); status=str(fixture.get('event_status') or '')
        incomplete=any(re.search(r'\b\w\.',n) for n in names)
        eid=await canonical('api_tennis',pid,'tennis',' vs '.join(names),start.isoformat(),{'participants':names,'participant_names_incomplete':incomplete,'tournament':fixture.get('tournament_name'),'source_url':'https://api-tennis.com/documentation'},status)
        observed=clock().isoformat()
        for i,(name,providerid) in enumerate(zip(names,ids)):
            key='api_tennis:tennis:'+str(providerid)
            basis=dict(sport='tennis',key=key,name=name,provider='api_tennis',url='https://api-tennis.com/documentation',effective=start.isoformat(),event_id=eid,observed=observed)
            rows.append(record(**basis,category='fixture',metric='participant',value={'provider_id':str(providerid),'kind':'player','identity_precision':'initialled' if incomplete else 'full_source_name'}))
            if status.lower()=='finished':
                win=fixture.get('event_winner')
                result=('win' if win==('First Player' if i==0 else 'Second Player') else 'loss') if win in ('First Player','Second Player') else 'unknown'
                rows.append(record(**basis,category='appearance',metric='completed_match',value={'result':result,'sets_played':len(fixture.get('scores') or []) or None,'match_duration_minutes':None,'minutes_played':None,'completed_observed_at':observed,'kind':'player'}))
            for stat in fixture.get('statistics') or []:
                if str(stat.get('player_key'))!=str(providerid) or stat.get('stat_period')!='match': continue
                rows.append(record(**basis,category='match_stat',metric=str(stat.get('stat_name') or 'unknown'),value={'value':stat.get('stat_value'),'won':stat.get('stat_won'),'total':stat.get('stat_total')}))
            if 'withdraw' in status.lower() or 'retir' in status.lower():
                # Match status does not identify the injured player or diagnose an injury.
                rows.append(record(**basis,category='availability',metric='match_status',value={'status':status,'affected_player_unknown':True},metadata={'confirmation_kind':'current_provider_state','not_medical_clearance':True}))
    return await save(rows)

async def sportmonks():
    """Optional football profiles, absences, and minutes with explicit source IDs."""
    from laboratory import canonical
    first=(clock()-timedelta(days=14)).date().isoformat(); last=(clock()+timedelta(days=2)).date().isoformat()
    url=f'https://api.sportmonks.com/v3/football/fixtures/between/{first}/{last}'
    total=0
    for page in range(1,41):
        payload=await get_json(url,{'api_token':os.environ['SPORTMONKS_API_TOKEN'],'include':'participants;lineups.player;lineups.details.type;sidelined.player;sidelined.type;state','per_page':25,'page':page})
        if not isinstance(payload.get('data'),list): raise RuntimeError('Sportmonks returned no fixture list; check plan access')
        rows=[]
        for fixture in payload['data']:
            ts=number(fixture.get('starting_at_timestamp')); participants=fixture.get('participants') or []
            if ts is None or len(participants)!=2: continue
            start=datetime.fromtimestamp(ts,UTC).isoformat(); names=[x.get('name') for x in participants]
            if not all(names): continue
            state=fixture.get('state') or {}; done=fixture.get('state_id') in (5,7,8)
            eid=await canonical('sportmonks',str(fixture['id']),'football',' vs '.join(names),start,{'participants':names,'competition':str(fixture.get('league_id')),'source_url':'https://www.sportmonks.com/'},state.get('state') or ('finished' if done else 'scheduled'))
            observed=clock().isoformat(); source='https://www.sportmonks.com/'
            teamkeys={str(p['id']):'sportmonks:football:team:'+str(p['id']) for p in participants}
            for team in participants:
                key=teamkeys[str(team['id'])]; name=team['name']
                rows.append(record('football',key,name,'fixture','participant',{'kind':'team','provider_id':str(team['id'])},'sportmonks',source,effective=start,event_id=eid,observed=observed))
                if done: rows.append(record('football',key,name,'appearance','completed_match',{'kind':'team','result':'unknown','minutes_played':None,'match_duration_minutes':number(fixture.get('length')),'completed_observed_at':observed},'sportmonks',source,effective=start,event_id=eid,observed=observed))
                if 'sidelined' in fixture:
                    absences=[x for x in fixture['sidelined'] or [] if str(x.get('team_id'))==str(team['id'])]
                    rows.append(record('football',key,name,'availability','reported_absences',{'status':'provider_absence_report','reported_absences':len(absences),'not_medical_clearance':True},'sportmonks',source,observed=observed,event_id=eid,metadata={'confirmation_kind':'current_provider_state'}))
            for lineup in fixture.get('lineups') or []:
                player=lineup.get('player') or {}; playerid=player.get('id') or lineup.get('player_id'); name=player.get('display_name') or player.get('name') or lineup.get('player_name')
                if not playerid or not name: continue
                key='sportmonks:football:player:'+str(playerid); teamkey=teamkeys.get(str(lineup.get('team_id')))
                profile={'displayHeight':str(player['height'])+' cm' if player.get('height') else None,'displayWeight':str(player['weight'])+' kg' if player.get('weight') else None,'dateOfBirth':player.get('date_of_birth')}
                pr=profile_records('football',key,name,profile,'sportmonks',source,observed)
                for r in pr: r['metadata']['team_key']=teamkey
                rows.extend(pr)
                minutes=None
                for stat in lineup.get('details') or []:
                    typ=stat.get('type') or {}; code=str(typ.get('developer_name') or typ.get('code') or typ.get('name') or '').lower().replace(' ','_').replace('-','_')
                    if code in ('minutes_played','minutesplayed'):
                        data=stat.get('data') or {}; minutes=number(data.get('value'))
                if done and minutes is not None:
                    rows.append(record('football',key,name,'appearance','completed_match',{'kind':'player','result':'unknown','minutes_played':minutes,'match_duration_minutes':None,'completed_observed_at':observed},'sportmonks',source,effective=start,event_id=eid,observed=observed,metadata={'team_key':teamkey}))
            for absent in fixture.get('sidelined') or []:
                player=absent.get('player') or {}; playerid=absent.get('player_id') or player.get('id'); name=player.get('display_name') or player.get('name')
                if not playerid or not name: continue
                value={'status':'returned' if absent.get('completed') else 'reported_unavailable','reason':(absent.get('type') or {}).get('name'),'start_date':absent.get('start_date'),'end_date':absent.get('end_date'),'completed':absent.get('completed')}
                rows.append(record('football','sportmonks:football:player:'+str(playerid),name,'availability','status',value,'sportmonks',source,observed=observed,event_id=eid,metadata={'confirmation_kind':'current_provider_state','team_key':teamkeys.get(str(absent.get('team_id')))}))
        total+=await save(rows)
        if not (payload.get('pagination') or {}).get('has_more'): return total
    raise RuntimeError('Sportmonks pagination budget reached; narrow configured coverage')
