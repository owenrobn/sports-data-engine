"""Unified SDE evidence envelope and deterministic market-quality checks."""
import asyncio
import json
import math
import os
import uuid
from datetime import timedelta
from urllib.parse import urlencode
from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from context_feed import clock,date,number
CTX={}

def book_quality(book,decision,notional=10):
    observed=date(book.get('observed_at'));age=(decision-observed).total_seconds() if observed else None
    def levels(side):
        valid=[]
        for r in book.get(side) or []:
            p=number(r.get('price'));q=number(r.get('size'))
            if p is not None and 0<p<1 and q is not None and q>0: valid.append((p,q))
        return sorted(valid,reverse=side=='bids')
    bids=levels('bids');asks=levels('asks');bid=bids[0][0] if bids else None;ask=asks[0][0] if asks else None
    crossed=bid is not None and ask is not None and bid>ask
    remaining=notional;shares=cost=0.
    for price,size in asks:
        spend=min(remaining,price*size);cost+=spend;shares+=spend/price;remaining-=spend
        if remaining<1e-9: break
    vwap=cost/shares if shares else None
    return {'token_id':book.get('token_id'),'receipt_id':book.get('id'),'observed_at':book.get('observed_at'),'age_seconds':round(age,2) if age is not None else None,'fresh':age is not None and 0<=age<=120,'best_bid':bid,'best_ask':ask,'spread':ask-bid if bid is not None and ask is not None else None,'crossed':crossed,'ask_depth_shares':sum(q for p,q in asks),'ask_depth_notional':round(sum(p*q for p,q in asks),4),'hypothetical_buy':{'requested_notional':notional,'filled_notional':round(cost,4),'complete':remaining<1e-9,'vwap':vwap,'price_slippage':vwap-ask if vwap is not None and ask is not None else None,'fees_included':False,'execution_performed':False},'usable':bool(asks and not crossed and age is not None and 0<=age<=120)}

def fair_odds(rows,expected,decision):
    """Never de-vig a partial or mixed-book market. Labels must be canonical outcomes."""
    latest={}
    for r in rows:
        t=date(r.get('observed_at'));effective=date(r.get('effective_at'))
        if not t or t>decision or (effective and effective>decision): continue
        if r.get('selection_key') not in expected: continue
        key=(r.get('provider'),r.get('bookmaker'),r.get('market_key'),r.get('line'),r.get('selection_key'))
        if key not in latest or t>date(latest[key]['observed_at']): latest[key]=r
    groups={}
    for k,r in latest.items(): groups.setdefault(k[:-1],{})[k[-1]]=r
    out=[]
    for k,g in groups.items():
        if set(g)!=set(expected): continue
        times=[date(r['observed_at']) for r in g.values()];odds={s:number(r.get('odds_decimal')) for s,r in g.items()}
        if any(v is None or v<=1 for v in odds.values()): continue
        sync=(max(times)-min(times)).total_seconds();age=(decision-min(times)).total_seconds()
        # Quote source time must exist for every outcome, not just recent retrieval.
        source_times=[date(r.get('effective_at')) for r in g.values()]
        source_fresh=all(t and 0<=(decision-t).total_seconds()<=120 for t in source_times)
        implied={s:1/v for s,v in odds.items()};total=sum(implied.values())
        out.append({'provider':k[0],'bookmaker':k[1],'market_key':k[2],'line':k[3],'outcomes':odds,'implied_probability_sum':total,'fair_probabilities':{s:v/total for s,v in implied.items()},'method':'multiplicative_normalization','synchronization_seconds':sync,'age_seconds':age,'usable':sync<=5 and age<=120 and source_fresh,'source_time_known':all(source_times),'sharp_benchmark_verified':False,'settlement_rules_verified':False,'receipt_ids':[r['id'] for r in g.values()]})
    return out

def live_fields(sport,state):
    p=(state or {}).get('participants') or [];status=(state or {}).get('status') or {}
    def found(names):
        values=[]
        for person in p:
            stats={str(s.get('name')):s for s in person.get('statistics') or []}
            item=next((stats[n] for n in names if n in stats),None)
            values.append(item)
        return values if len(values)==2 and all(v is not None for v in values) else None
    scores=[x.get('score') for x in p]
    fields={'score':scores if len(scores)==2 and all(x is not None for x in scores) else None}
    if sport=='football':
        fields.update(clock=status.get('displayClock'),shots_on_target=found(['shotsOnTarget']),dangerous_attacks=found(['dangerousAttacks']),red_cards=found(['redCards']),xg=found(['expectedGoals','xG']))
    else:
        fields.update(sets=[x.get('sets') for x in p] if p and all(x.get('sets') is not None for x in p) else None,server=next((x.get('provider_id') for x in p if x.get('server') is True),None),serve_points=found(['servicePointsWon']),return_points=found(['returnPointsWon']),tiebreak=None)
    return {'fields':fields,'missing_fields':[k for k,v in fields.items() if v is None],'source_definitions_validated':False}

async def get(table,**q): return await CTX['db']('GET',table,query='?'+urlencode(q)) or []

async def envelope(event_id,as_of=None):
    from context_feed import package as context_package
    from live_state import package as live_package
    decision=date(as_of) if as_of else clock()
    if not decision or decision>clock(): raise HTTPException(422,'Use a timezone-aware decision time no later than now')
    context,live=await asyncio.gather(context_package(event_id,decision.isoformat()),live_package(event_id,decision.isoformat()))
    sport=live['sport'];cutoff=date(context['decision_cutoff'])
    links,books,odds,summaries,weather=await asyncio.gather(get('market_match_links',event_id='eq.'+str(event_id),created_at='lte.'+decision.isoformat(),limit=100),get('prediction_market_books',event_id='eq.'+str(event_id),observed_at='lte.'+decision.isoformat(),order='observed_at.desc',limit=500),get('odds_snapshots',event_id='eq.'+str(event_id),observed_at='lte.'+decision.isoformat(),order='observed_at.desc',limit=500),get('event_snapshots',event_id='eq.'+str(event_id),provider='eq.espn_summary',observed_at='lte.'+cutoff.isoformat(),order='observed_at.desc',limit=1),get('event_snapshots',event_id='eq.'+str(event_id),provider='eq.open_meteo',observed_at='lte.'+cutoff.isoformat(),order='observed_at.desc',limit=1))
    markets=[]
    for link in links:
        catalog=await get('polymarket_market_catalog',market_id='eq.'+link['market_id'],limit=1)
        meta=(catalog[0].get('metadata') or {}) if catalog else {};tokens=meta.get('clobTokenIds') or [];outcomes=meta.get('outcomes') or []
        # Current catalog metadata is not historical proof of past token mapping.
        mapping_known=bool(catalog and date(catalog[0].get('observed_at')) and date(catalog[0]['observed_at'])<=decision)
        selected={}
        for book in books:
            if book['market_id']==link['market_id']: selected.setdefault(str(book['token_id']),book)
        quotes=[dict(book_quality(selected[str(t)],decision),outcome=outcomes[i] if i<len(outcomes) else None) for i,t in enumerate(tokens) if str(t) in selected]
        times=[date(q['observed_at']) for q in quotes]
        sync=(max(times)-min(times)).total_seconds() if times else None
        complete=bool(mapping_known and tokens and len(quotes)==len(tokens) and len(outcomes)==len(tokens) and len(set(map(str,tokens)))==len(tokens))
        markets.append({'market_id':link['market_id'],'question':link.get('market_question'),'market_type':link.get('market_type'),'link_receipt':link,'quotes':quotes,'all_outcomes_present':complete,'synchronization_seconds':sync,'executable_snapshot_usable':complete and all(q['usable'] for q in quotes) and sync<=5,'settlement_rules_verified':False,'sharp_benchmark_verified':False})
    lf=live_fields(sport,live.get('snapshot'))
    blockers=list(context['missing_required_inputs'])
    blockers+=['independent_fixture_verification','sharp_benchmark_and_settlement_verification','private_account_integration','complete_ledger_capture_and_settlement_history']
    if not markets: blockers.append('verified_market_links_and_executable_books')
    if not (live.get('snapshot') or {}).get('retrieval_fresh'): blockers.append('fresh_live_snapshot')
    blockers+=['live.'+x for x in lf['missing_fields']]
    return {'schema_version':'2.0','event_id':str(event_id),'match':context['match'],'sport':sport,'decision_time':decision.isoformat(),'pre_match_cutoff':context['decision_cutoff'],'engine_ready':False,'readiness_scope':'SDE evidence contract, not a prediction or trade authorization','blockers':sorted(set(blockers)),'recent_performance':context,'live_state':live,'normalized_live_inputs':lf,'pre_match_source_summary':summaries[0] if summaries else None,'weather_receipt':weather[0] if weather else None,'markets':markets,'bookmaker_complete_sets':fair_odds(odds,['home','draw','away'] if sport=='football' else ['player1','player2'],decision),'private_account_data':None,'capture_policy':{'performance_days':[7,14],'live_max_retrieval_age_seconds':120,'quotes_max_age_seconds':120,'outcome_synchronization_seconds':5,'historical_catalog_mapping':'unknown unless archived by decision time','actual_kickoff_verified':False},'requirements':context['rae_requirements']}

async def operations():
    runs=await get('sde_provider_runs',order='started_at.desc',limit=250)
    latest={}
    for r in runs: latest.setdefault((r['sport'],r['provider']),r)
    providers=[]
    for (sport,provider),r in latest.items():
        start=date(r.get('started_at'));age=(clock()-start).total_seconds() if start else None
        providers.append({'sport':sport,'provider':provider,'status':r['status'],'records':r.get('records'),'age_seconds':round(age) if age is not None else None,'stalled':bool(r['status']=='running' and age is not None and age>660),'last_error':r.get('error'),'started_at':r.get('started_at'),'finished_at':r.get('finished_at')})
    from context_feed import provider_status
    return {'generated_at':clock().isoformat(),'providers':providers,'access':provider_status(),'blockers':['API Tennis and Sportmonks credentials are needed for their inactive feeds.','Independent fixture/benchmark feeds and private account access are not configured.','Continuous hosting and database capacity need an infrastructure plan; no paid upgrades were applied.'],'service_continuity':'Collectors run while the service is awake. Source errors and missing data are not successful coverage.'}

def install(ctx):
    CTX.update(ctx);app=ctx['app']
    from laboratory import page,table,esc
    @app.get('/data/engine/{event_id}')
    async def data(event_id:uuid.UUID,as_of:str|None=None): return await envelope(event_id,as_of)
    @app.get('/engine/{event_id}',response_class=HTMLResponse)
    async def view(event_id:uuid.UUID):
        p=await envelope(event_id)
        body='<p><a href="/context?sport='+p['sport']+'">Match list</a> · <a href="/data/engine/'+str(event_id)+'">Combined engine JSON</a> · <a href="/operations">Operations</a></p><p>Decision time: '+esc(p['decision_time'])+'. Engine ready: False.</p>'
        body+=table(['Missing or unverified input'],[[esc(x)] for x in p['blockers']])
        body+='<details><summary>All evidence and receipts</summary><pre>'+esc(json.dumps(p,indent=2))+'</pre></details>'
        return page(p['match']+' · Unified SDE evidence',body)
    @app.get('/data/operations')
    async def health(): return await operations()
    @app.get('/operations',response_class=HTMLResponse)
    async def ops():
        p=await operations();body='<p><a href="/context">Match packages</a> · <a href="/rae">Full requirements</a> · <a href="/data/operations">Health JSON</a></p><p>'+esc(p['service_continuity'])+'</p>'
        body+=table(['Sport','Collector','Last status','Records','Age (seconds)','Stalled','Error'],[[esc(r[k]) for k in ('sport','provider','status','records','age_seconds','stalled','last_error')] for r in p['providers']])
        body+='<h2>Access and infrastructure blockers</h2><ul>'+''.join('<li>'+esc(x)+'</li>' for x in p['blockers'])+'</ul>'
        return page('SDE · Operations and remaining access',body)
