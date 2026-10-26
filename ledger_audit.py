from runtime_policy import collection_paused
"""Receipt-first LEDGER audit. No orders, fills or bankroll values are fabricated."""
import asyncio
import hmac
import json
import math
import os
import uuid
import httpx
from datetime import datetime,timedelta
from urllib.parse import urlencode
from fastapi import Header,HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel,ConfigDict,Field
from context_feed import clock,date,number,record,save
CTX={}

async def get(table,**q): return await CTX['db']('GET',table,query='?'+urlencode(q)) or []
def array(v):
    if isinstance(v,list): return v
    try:
        parsed=json.loads(v or '[]');return parsed if isinstance(parsed,list) else []
    except (ValueError,TypeError): return []

def resolution(gamma,state):
    outcomes=array(gamma.get('outcomes'));tokens=[str(t) for t in array(gamma.get('clobTokenIds'))];prices=[number(x) for x in array(gamma.get('outcomePrices'))]
    condition=gamma.get('conditionId');payouts=[number(x) for x in state.get('payouts') or []]
    one_hot=lambda values:len(values)>=2 and all(v in (0,1) for v in values) and sum(values)==1
    mapping_ok=len(tokens)==len(outcomes)==len(prices) and len(set(tokens))==len(tokens)
    terminal=gamma.get('closed') is True and gamma.get('umaResolutionStatus')=='resolved' and state.get('status')=='resolved' and state.get('condition_id')==condition
    agreement=one_hot(prices) and one_hot(payouts) and prices==payouts
    ready=bool(mapping_ok and terminal and agreement)
    return {'status':'provider_confirmed_one_hot' if ready else 'unconfirmed_or_non_binary_payout','condition_id':condition,'market_id':str(gamma.get('id')),'token_outcomes':dict(zip(tokens,outcomes)) if mapping_ok else {},'winning_token':tokens[prices.index(1)] if ready else None,'payouts_by_token':dict(zip(tokens,payouts)) if ready else None,'provider_reported_resolved_at':state.get('resolved_at'),'transaction_hash':state.get('transaction_hash'),'on_chain_independently_verified':False,'scorable':ready,'gamma_status':gamma.get('umaResolutionStatus'),'resolution_status':state.get('status'),'note':'Closed markets and terminal-looking prices alone are not accepted as resolutions. Split/refund payouts are retained but not scored as one-hot results.'}

def brier(probabilities,payouts):
    if set(probabilities)!=set(payouts) or len(probabilities)<2: raise ValueError('Outcome sets differ')
    p={k:number(v) for k,v in probabilities.items()};y={k:number(v) for k,v in payouts.items()}
    if any(v is None or not 0<=v<=1 for v in p.values()) or not math.isclose(sum(p.values()),1,abs_tol=1e-6): raise ValueError('Invalid probability vector')
    if any(v not in (0,1) for v in y.values()) or sum(y.values())!=1: raise ValueError('Result must be one-hot')
    # Sum across mutually exclusive outcomes; binary scalar Brier is half this value.
    return {'multiclass_sum':sum((p[k]-y[k])**2 for k in p),'binary_scalar':sum((p[k]-y[k])**2 for k in p)/2 if len(p)==2 else None,'definition':'sum of squared outcome probability errors; binary_scalar is the single binary-event convention'}

def capture_audit(captures,books,start,as_of):
    valid=[]
    for receipt in captures:
        payload=receipt.get('payload') or {};finish=date(payload.get('capture_finished_at'));observed=date(receipt.get('observed_at'));begin=date(payload.get('capture_started_at'))
        if not finish or not begin or not observed or begin>finish or finish>observed or observed>as_of: continue
        tokens=list(map(str,payload.get('tokens') or []));outcomes=payload.get('outcomes') or []
        if len(tokens)<2 or len(tokens)!=len(outcomes) or len(set(tokens))!=len(tokens): continue
        quotes=[]
        for token in tokens:
            matching=[b for b in books if b.get('market_id')==payload.get('market_id') and str(b.get('token_id'))==token and date(b.get('observed_at')) and begin<=date(b['observed_at'])<=finish]
            if not matching: break
            from engine_delivery import book_quality
            b=max(matching,key=lambda b:date(b['observed_at']));q=book_quality(b,finish)
            if b.get('provider_timestamp_required'):
                source_time=date(b.get('provider_event_time'));q['source_fresh']=source_time is not None and 0<=(finish-source_time).total_seconds()<=120
                q['usable']=q['usable'] and q['source_fresh']
            quotes.append(q)
        times=[date(q['observed_at']) for q in quotes]
        sync=(max(times)-min(times)).total_seconds() if times else None
        usable=len(quotes)==len(tokens) and all(q['usable'] for q in quotes) and sync is not None and sync<=5
        valid.append({'receipt_id':receipt['id'],'market_id':payload.get('market_id'),'observed_at':observed.isoformat(),'phase':payload.get('phase'),'mapping':dict(zip(tokens,outcomes)),'quotes':quotes,'complete_synchronized_book':usable,'synchronization_seconds':sync,'seconds_before_current_scheduled_start':(start-finish).total_seconds() if start else None})
    valid.sort(key=lambda x:x['observed_at'])
    usable=[r for r in valid if r['complete_synchronized_book']]
    early=[r for r in usable if r['seconds_before_current_scheduled_start'] is not None and r['seconds_before_current_scheduled_start']>600]
    near=[r for r in usable if r['seconds_before_current_scheduled_start'] is not None and 0<=r['seconds_before_current_scheduled_start']<=600]
    return {'captures_observed':len(valid),'usable_captures':len(usable),'early_capture':early[0] if early else None,'scheduled_pre_10m_capture':near[-1] if near else None,'actual_pre_10m_capture':None,'actual_start_verified':False,'closing_line_value':None,'closing_line_status':'requires verified actual start, compatible settlement rules and complete benchmark quotes','captures':valid,'schedule_policy':'Window positions use the current scheduled start at audit time; original phase and scheduled start remain in source receipts. A reschedule does not establish actual kickoff.'}

async def collect(sport):
    from laboratory import sid_for,put,tracked
    from context_feed import get_json
    sid=await sid_for(sport);now=clock()
    events=await get('events',select='id,name,starts_at',sport_id='eq.'+sid,starts_at='gte.'+(now-timedelta(days=14)).isoformat(),**{'and':'(starts_at.lte.'+(now+timedelta(days=1)).isoformat()+')'},order='starts_at.desc',limit=500)
    if not events: return 0
    links=await get('market_match_links',event_id='in.('+','.join(e['id'] for e in events)+')',order='id.asc',limit=1000)
    cursor=await get('backfill_checkpoints',provider='eq.ledger_resolution',stream='eq.'+sport,cursor_key='eq.market_offset',limit=1)
    offset=int(cursor[0].get('cursor_value') or 0) if cursor else 0
    if offset>=len(links): offset=0
    batch=links[offset:offset+12];total=0
    for link in batch:
        async def one():
            market=await get_json('https://gamma-api.polymarket.com/markets/'+str(link['market_id']))
            if str(market.get('id'))!=str(link['market_id']) or market.get('conditionId')!=link.get('condition_id'): raise ValueError('Market identity mismatch')
            state_error=None
            try: state_payload=await get_json('https://data-api.polymarket.com/v2/resolutions',{'condition':link['condition_id']})
            except httpx.HTTPError as exc:
                state_error=exc
                state_payload={'data':[]}
            states=[s for s in state_payload.get('data') or [] if s.get('condition_id')==link['condition_id']]
            state=states[0] if len(states)==1 else {}
            fields=('id','question','conditionId','outcomes','clobTokenIds','outcomePrices','closed','umaResolutionStatus','closedTime','updatedAt','resolutionSource','description')
            payload={'market':{k:market[k] for k in fields if k in market},'resolution_state':state,'assessment':resolution(market,state),'source_urls':['https://gamma-api.polymarket.com/markets/'+str(link['market_id']),'https://data-api.polymarket.com/v2/resolutions']}
            if state_error is not None: payload['resolution_state_fetch_error']={'class':type(state_error).__name__,'http_status':state_error.response.status_code if isinstance(state_error,httpx.HTTPStatusError) else None}
            # Append only changed states; identical unresolved polling does not inflate storage.
            previous=await get('event_snapshots',event_id='eq.'+link['event_id'],provider='eq.ledger_resolution',**{'payload->market->>id':'eq.'+str(link['market_id'])},order='observed_at.desc',limit=1)
            if previous and previous[0].get('payload')==payload:
                if state_error is not None: raise state_error
                return 0
            await put('event_snapshots',{'event_id':link['event_id'],'provider':'ledger_resolution','observed_at':clock().isoformat(),'effective_at':None,'quality':None,'payload':payload})
            if state_error is not None: raise state_error
            return 1
        total+=(await tracked('ledger_resolution_market',sport,one)) or 0
    await put('backfill_checkpoints',{'provider':'ledger_resolution','stream':sport,'cursor_key':'market_offset','cursor_value':str(offset+len(batch)),'status':'running','updated_at':clock().isoformat()},'provider,stream,cursor_key',True)
    return total

async def report(event_id,as_of=None,private=False):
    if collection_paused():
        from fresh_engine import ledger
        return ledger(event_id,as_of,private)
    cutoff=date(as_of) if as_of else clock()
    if not cutoff or cutoff>clock(): raise HTTPException(422,'Use a timezone-aware as_of no later than now')
    events=await get('events',id='eq.'+str(event_id),limit=1)
    if not events: raise HTTPException(404,'Match not found')
    e=events[0];start=date(e.get('starts_at'))
    from laboratory import sid_for,pages
    sport='football' if str(e['sport_id'])==str(await sid_for('football')) else 'tennis'
    # Complete bounded pagination avoids silently declaring a partial quote sample complete.
    captures,books,states=await asyncio.gather(pages('event_snapshots',event_id='eq.'+str(event_id),provider='eq.clob_capture',observed_at='lte.'+cutoff.isoformat(),order='observed_at.asc,id.asc'),pages('prediction_market_books',event_id='eq.'+str(event_id),observed_at='lte.'+cutoff.isoformat(),order='observed_at.asc,id.asc'),pages('event_snapshots',event_id='eq.'+str(event_id),provider='eq.ledger_resolution',observed_at='lte.'+cutoff.isoformat(),order='observed_at.asc,id.asc'))
    latest={}
    for receipt in states:
        mid=str((receipt.get('payload') or {}).get('market',{}).get('id'))
        latest[mid]=receipt
    audit=capture_audit(captures,books,start,cutoff)
    out={'event_id':str(event_id),'match':e['name'],'sport':sport,'audit_as_of':cutoff.isoformat(),'scheduled_start':e.get('starts_at'),'ledger_complete':False,'capture_audit':audit,'resolution_receipts':list(latest.values()),'actual_finish_time':None,'result_within_24h_verified':False,'realized_pnl':None,'account_integration':'not_configured','private_forecasts_included':private,'limitations':['Venue resolution is not proof of a wallet payout or realized PnL.','No forecasts or historical quotes are invented to fill audit gaps.','Current event scheduling metadata is not a reconstruction of a historical schedule.']}
    if private:
        forecasts=await get('sde_context_evidence',event_id='eq.'+str(event_id),category='eq.collection',metric='eq.ledger_forecast',observed_at='lte.'+cutoff.isoformat(),order='observed_at.asc',limit=500)
        scored=[]
        for f in forecasts:
            v=f.get('value') or {};receipt=latest.get(str(v.get('market_id')));assessment=(receipt.get('payload') or {}).get('assessment',{}) if receipt else {}
            score=None;reason='awaiting confirmed one-hot resolution'
            # No retrospective submission is scored, and retracted schedule assurance is not assumed.
            if start and date(f['observed_at'])>=start: reason='forecast was not archived before the current scheduled start'
            elif assessment.get('scorable'):
                try: score=brier(v['probabilities'],assessment['payouts_by_token']);reason=None
                except ValueError: reason='forecast and resolved token sets differ'
            scored.append({'forecast_id':f['id'],'archived_at':f['observed_at'],'model':v.get('model'),'model_version':v.get('model_version'),'market_id':v.get('market_id'),'brier':score,'not_scored_reason':reason,'resolution_receipt_id':receipt.get('id') if receipt else None,'pre_actual_start_verified':False,'evaluation_scope':'provisional scheduled-start cutoff; actual-start validation remains required'})
        out['forecast_scores']=scored
    return out

class Forecast(BaseModel):
    model_config=ConfigDict(extra='forbid')
    forecast_id:uuid.UUID
    event_id:uuid.UUID
    market_id:str=Field(pattern=r'^\d+$',max_length=60)
    model:str=Field(min_length=1,max_length=100)
    model_version:str=Field(min_length=1,max_length=100)
    inputs_as_of:datetime
    probabilities:dict[str,float]

def authenticate(token):
    expected=os.getenv('SDE_ADMIN_TOKEN','')
    if not expected or not hmac.compare_digest(expected.encode(),token.encode()): raise HTTPException(403,'Administrator authentication required')

async def archive_forecast(f):
    received=clock()
    if f.inputs_as_of.tzinfo is None or f.inputs_as_of>received: raise HTTPException(422,'inputs_as_of must be aware and no later than receipt time')
    probs=f.probabilities
    if len(probs)<2 or len(probs)>10 or any(number(p) is None or not 0<=p<=1 for p in probs.values()) or not math.isclose(sum(probs.values()),1,abs_tol=1e-6): raise HTTPException(422,'Supply a complete probability vector summing to one')
    events=await get('events',id='eq.'+str(f.event_id),limit=1)
    if not events: raise HTTPException(404,'Match not found')
    e=events[0];start=date(e.get('starts_at'))
    if not start or received>=start: raise HTTPException(422,'Forecast must be archived before the scheduled match start')
    links=await get('market_match_links',event_id='eq.'+str(f.event_id),market_id='eq.'+f.market_id,limit=1)
    if not links: raise HTTPException(422,'Market is not linked to this match')
    catalog=await get('polymarket_market_catalog',market_id='eq.'+f.market_id,limit=1)
    if not catalog or catalog[0].get('closed'): raise HTTPException(422,'Open market mapping required')
    tokens=(catalog[0].get('metadata') or {}).get('clobTokenIds') or []
    if set(map(str,tokens))!=set(probs) or len(tokens)!=len(probs): raise HTTPException(422,'Probability keys must exactly match all market token IDs')
    from laboratory import sid_for
    sport='football' if str(e['sport_id'])==str(await sid_for('football')) else 'tennis'
    latest=await get(sport+'_match_events',event_id='eq.'+str(f.event_id),provider='eq.espn',order='observed_at.desc',limit=1)
    status=((latest[0].get('payload') or {}).get('status') or {}).get('type') or {} if latest else {}
    if status.get('state') in ('in','post') or status.get('completed'): raise HTTPException(422,'Source already reports this match in progress or completed')
    value={'market_id':f.market_id,'model':f.model,'model_version':f.model_version,'inputs_as_of':f.inputs_as_of.isoformat(),'probabilities':probs,'archive_policy':'decision time is server receipt time; no retrospective backdating','actual_start_verified':False}
    row=record(sport,'ledger:forecast:'+str(f.event_id),e['name'],'collection','ledger_forecast',value,'model_receipt','https://sports-data-engine.onrender.com/ledger/forecasts',event_id=str(f.event_id),observed=received.isoformat(),effective=received.isoformat(),metadata={'private_model_receipt':True})
    row['id']=str(uuid.uuid5(uuid.NAMESPACE_URL,'sde-forecast:'+str(f.forecast_id)))
    existing=await get('sde_context_evidence',id='eq.'+row['id'],limit=1)
    if existing:
        if existing[0].get('value')!=value or existing[0].get('event_id')!=str(f.event_id): raise HTTPException(409,'Forecast ID already has different immutable contents')
        return {'forecast_id':row['id'],'status':'already_archived','observed_at':existing[0]['observed_at']}
    await save([row])
    archived=await get('sde_context_evidence',id='eq.'+row['id'],limit=1)
    if not archived or archived[0].get('value')!=value or archived[0].get('event_id')!=str(f.event_id): raise HTTPException(409,'Concurrent forecast ID conflict; retained record was not overwritten')
    return {'forecast_id':row['id'],'status':'archived','observed_at':row['observed_at'],'actual_start_verified':False}

def install(ctx):
    CTX.update(ctx);app=ctx['app']
    @app.post('/ledger/forecasts')
    async def forecasts(f:Forecast,x_sde_admin_token:str=Header(default='')):
        authenticate(x_sde_admin_token);return await archive_forecast(f)
    @app.get('/data/ledger/private/{event_id}')
    async def private(event_id:uuid.UUID,as_of:str|None=None,x_sde_admin_token:str=Header(default='')):
        authenticate(x_sde_admin_token);return await report(event_id,as_of,private=True)
    @app.get('/data/ledger/{event_id}')
    async def data(event_id:uuid.UUID,as_of:str|None=None): return await report(event_id,as_of)
    @app.get('/ledger/{event_id}',response_class=HTMLResponse)
    async def view(event_id:uuid.UUID):
        from laboratory import page,esc,table
        p=await report(event_id);a=p['capture_audit']
        body='<p><a href="/engine/'+str(event_id)+'">Combined evidence</a> · <a href="/data/ledger/'+str(event_id)+'">Public audit JSON</a> · <a href="/operations">Operations</a></p><p>Ledger complete: False. Private forecasts and account data are excluded from this public page.</p>'
        body+=table(['Captures observed','Complete synchronized captures','Actual start verified','Closing line verified'],[[a['captures_observed'],a['usable_captures'],'No','No']])
        body+=table(['Market','Resolution assessment','Observed'],[[esc((r.get('payload') or {}).get('market',{}).get('id')),esc((r.get('payload') or {}).get('assessment',{}).get('status')),esc(r['observed_at'])] for r in p['resolution_receipts']])
        body+='<details><summary>Capture and resolution receipts</summary><pre>'+esc(json.dumps(p,indent=2))+'</pre></details>'
        return page(p['match']+' · LEDGER audit',body)
    @app.on_event('startup')
    async def start():
        if collection_paused() or not ctx['SUPABASE_KEY']: return
        from laboratory import BACKGROUND,tracked
        async def lane(sport):
            while True:
                await tracked('ledger_resolution',sport,lambda:collect(sport))
                await asyncio.sleep(900)
        for sport in ('football','tennis'): BACKGROUND.append(asyncio.create_task(lane(sport)))
