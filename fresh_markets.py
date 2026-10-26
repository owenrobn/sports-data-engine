"""Bounded public market evidence for the database-independent cloud lane."""
import asyncio
import json
import copy
from datetime import datetime,timezone,timedelta
from context_feed import clock,date,get_json,number
from laboratory import arr,match_candidate,uid

SEARCH='https://gamma-api.polymarket.com/public-search'
BOOK='https://clob.polymarket.com/book'

def mapping(m):
    # Gamma's V2 markets expose position IDs; CTF markets expose token IDs.
    tokens=arr(m.get('positionIds')) if m.get('version')=='v2' else arr(m.get('clobTokenIds'))
    outcomes=arr(m.get('outcomes'))
    if not isinstance(tokens,list) or not isinstance(outcomes,list): return None
    tokens=list(map(str,tokens));outcomes=list(map(str,outcomes))
    if not 2<=len(tokens)<=3 or len(tokens)!=len(outcomes) or len(set(tokens))!=len(tokens) or len(set(outcomes))!=len(outcomes) or any(not t.isdigit() for t in tokens): return None
    return tokens,outcomes

def normalized_book(raw,token,condition,observed):
    if str(raw.get('asset_id'))!=token or raw.get('market')!=condition: raise ValueError('Book identity mismatch')
    sides={}
    for side in ('bids','asks'):
        levels=raw.get(side)
        if not isinstance(levels,list) or len(levels)>2000: raise ValueError('Invalid or oversized book')
        valid=[]
        for row in levels:
            p=number(row.get('price'));q=number(row.get('size'))
            if p is None or q is None or not 0<p<1 or q<=0: raise ValueError('Invalid book level')
            valid.append({'price':str(row['price']),'size':str(row['size'])})
        sides[side]=valid
    try: source_time=datetime.fromtimestamp(float(raw['timestamp'])/1000,timezone.utc).isoformat()
    except (ValueError,TypeError,KeyError,OverflowError,OSError): source_time=None
    return {'id':uid('fresh-book:'+token+':'+observed),'token_id':token,'condition_id':condition,'observed_at':observed,'provider_event_time':source_time,'provider_timestamp_required':True,'hash':raw.get('hash'),'min_order_size':raw.get('min_order_size'),'tick_size':raw.get('tick_size'),'neg_risk':raw.get('neg_risk'),'source_url':BOOK,**sides}

async def capture(market):
    mapped=mapping(market)
    if not mapped: raise ValueError('Complete supported outcome mapping required')
    tokens,outcomes=mapped;condition=market.get('conditionId')
    if not condition: raise ValueError('Condition identity required')
    begin=clock().isoformat()
    async def one(token):
        raw=await get_json(BOOK,{'token_id':token})
        return normalized_book(raw,token,condition,clock().isoformat())
    results=await asyncio.gather(*(one(token) for token in tokens),return_exceptions=True)
    books=[r for r in results if not isinstance(r,Exception)]
    errors=[{'token_id':tokens[i],'error_class':type(r).__name__} for i,r in enumerate(results) if isinstance(r,Exception)]
    finish=clock().isoformat()
    return {'market_id':str(market['id']),'condition_id':condition,'question':market.get('question'),'market_type':market.get('sportsMarketType'),'version':market.get('version'),'tokens':tokens,'outcomes':outcomes,'mapping_observed_at':market['mapping_observed_at'],'capture_started_at':begin,'capture_finished_at':finish,'observed_at':finish,'books':books,'errors':errors,'description':market.get('description'),'resolution_source':market.get('resolutionSource'),'settlement_rules_verified':False,'sharp_benchmark_verified':False,'orders_submitted':False}

async def collect(sport,state):
    from fresh_feed import canonical
    lane=copy.deepcopy(state.get('markets') or {'searches':{},'receipts':{},'history':[]})
    matches={m['id']:m for p in state['packs'].values() for m in p['matches']}
    now=clock();eligible={k:m for k,m in matches.items() if date(m.get('starts_at')) and now-timedelta(hours=6)<=date(m['starts_at'])<=now+timedelta(days=2) and not (m['status'].get('type') or {}).get('completed')}
    lane['searches']={k:v for k,v in lane['searches'].items() if k in eligible}
    lane['receipts']={k:v for k,v in lane['receipts'].items() if k in eligible}
    candidates=sorted(eligible.values(),key=lambda m:abs((date(m['starts_at'])-now).total_seconds()))
    searches=0
    for m in candidates:
        old=lane['searches'].get(m['id'],{});checked=date(old.get('attempted_at'))
        if checked and (now-checked).total_seconds()<1800: continue
        if searches>=4: break
        searches+=1;attempted=clock().isoformat()
        try:
            raw=await get_json(SEARCH,{'q':m['name'],'limit_per_type':10,'events_status':'active'})
            accepted=[]
            for event in (raw.get('events') or [])[:10]:
                for market in (event.get('markets') or [])[:30]:
                    start=market.get('gameStartTime') or event.get('startTime')
                    cat={'start_time':start,'question':market.get('question'),'metadata':{'event_title':event.get('title')}}
                    e={'name':m['name'],'starts_at':m['starts_at'],'metadata':{'participants':[p['name'] for p in m['participants']]}}
                    candidate=match_candidate(e,cat)
                    if not candidate or not candidate['accepted'] or market.get('closed') is not False or not mapping(market): continue
                    # A market must match exactly one currently known fixture.
                    matches_to=[x for x in eligible.values() if (match_candidate({'name':x['name'],'starts_at':x['starts_at'],'metadata':{'participants':[p['name'] for p in x['participants']]}},cat) or {}).get('accepted')]
                    if len(matches_to)!=1: continue
                    accepted.append({k:market.get(k) for k in ('id','conditionId','question','sportsMarketType','version','positionIds','clobTokenIds','outcomes','description','resolutionSource')}|{'mapping_observed_at':clock().isoformat(),'link':{'source_url':SEARCH,'query':m['name'],'event_title':event.get('title'),'provider_event_id':event.get('id'),'source_match_start':start,'method':'exact_pair_time_v2','time_distance_minutes':candidate['distance'],'independent_fixture_verification':False}})
            unique={str(x['id']):x for x in accepted if x.get('id')}
            lane['searches'][m['id']]={'attempted_at':attempted,'source_url':SEARCH,'query':m['name'],'markets':list(unique.values())[:6],'search_complete':False}
        except Exception as ex: lane['searches'][m['id']]={**old,'attempted_at':attempted,'last_error_class':type(ex).__name__}
    targets=[]
    for eid,search in lane['searches'].items():
        for market in search.get('markets',[]):
            old=lane['receipts'].get(eid,{}).get(str(market['id']),{})
            t=date(old.get('observed_at'))
            targets.append((t or datetime.min.replace(tzinfo=timezone.utc),eid,market))
    for _,eid,market in sorted(targets,key=lambda x:x[0])[:4]:
        try:
            receipt=await capture(market);receipt['link']=market['link'];receipt['event_id']=eid
            if len(canonical(receipt))>128*1024: raise ValueError('Market receipt exceeds byte budget')
            lane['receipts'].setdefault(eid,{})[receipt['market_id']]=receipt
            lane['history'].append(receipt)
        except Exception as ex: lane.setdefault('errors',{})[str(market['id'])]={'attempted_at':clock().isoformat(),'error_class':type(ex).__name__}
    # Current snapshots are bounded; earlier immutable R2 snapshots keep older receipts.
    lane['history']=[r for r in lane['history'] if date(r.get('observed_at')) and date(r['observed_at'])>=now-timedelta(days=14)][-40:]
    while len(canonical(lane))>1024*1024 and lane['history']: lane['history'].pop(0)
    while len(canonical(lane))>1024*1024 and lane['searches']:
        oldest=min(lane['searches'],key=lambda k:lane['searches'][k].get('attempted_at',''))
        lane['searches'].pop(oldest);lane['receipts'].pop(oldest,None)
    if len(canonical(lane))>1024*1024: raise ValueError('Market state exceeds byte budget')
    lane['errors']={k:v for k,v in lane.get('errors',{}).items() if k in {str(m['id']) for search in lane['searches'].values() for m in search.get('markets',[])}}
    state['markets']=lane

def assess(receipt,decision):
    from engine_delivery import book_quality
    tokens=receipt['tokens'];outcomes=receipt['outcomes'];books=receipt['books']
    quotes=[]
    for i,token in enumerate(tokens):
        book=next((b for b in books if b['token_id']==token and date(b['observed_at'])<=decision),None)
        if not book: continue
        q=book_quality(book,decision);source_time=date(book.get('provider_event_time'))
        q['provider_event_time']=book.get('provider_event_time');q['source_fresh']=source_time is not None and 0<=(decision-source_time).total_seconds()<=120
        q['usable']=q['usable'] and q['source_fresh'];q['outcome']=outcomes[i];quotes.append(q)
    times=[date(q['observed_at']) for q in quotes];sync=(max(times)-min(times)).total_seconds() if times else None
    complete=len(quotes)==len(tokens) and date(receipt['mapping_observed_at'])<=decision
    return {**{k:v for k,v in receipt.items() if k!='books'},'quotes':quotes,'all_outcomes_present':complete,'synchronization_seconds':sync,'executable_snapshot_usable':bool(complete and all(q['usable'] for q in quotes) and sync<=5),'trade_authorized':False}
