from runtime_policy import collection_paused
"""Optional quota-bounded bookmaker feed with exact fixture matching."""
import asyncio
import os
from datetime import timedelta
from context_feed import clock,date,get_json,number
CTX={}

def match_fixture(remote,events):
    from laboratory import norm
    names=[remote.get('home_team'),remote.get('away_team')];t=date(remote.get('commence_time'))
    if not all(names) or not t: return None
    pair=sorted(map(norm,names));matches=[]
    for e in events:
        local=(e.get('metadata') or {}).get('participants') or []
        start=date(e.get('starts_at'))
        if len(local)==2 and sorted(map(norm,local))==pair and start and abs((start-t).total_seconds())<=900: matches.append(e)
    return matches[0] if len(matches)==1 else None

def normalize(remote,event,sport,observed,source):
    from laboratory import norm
    rows=[];names=(event.get('metadata') or {}).get('participants') or []
    for book in remote.get('bookmakers') or []:
        for market in book.get('markets') or []:
            if market.get('key')!='h2h': continue
            updated=market.get('last_update') or book.get('last_update')
            for outcome in market.get('outcomes') or []:
                name=outcome.get('name');odds=number(outcome.get('price'))
                if name=='Draw' and sport=='football': label='draw'
                elif sport=='football' and name in (remote.get('home_team'),remote.get('away_team')): label='home' if name==remote['home_team'] else 'away'
                elif sport=='tennis' and len(names)==2 and norm(name) in [norm(n) for n in names]: label='player'+str([norm(n) for n in names].index(norm(name))+1)
                else: continue
                if odds is None or odds<=1: continue
                rows.append({'event_id':event['id'],'provider':'the_odds_api','bookmaker':book.get('key'),'market_key':'h2h','selection_key':label,'odds_decimal':odds,'probability':None,'line':None,'observed_at':observed,'effective_at':updated if date(updated) else None,'metadata':{'source_url':source,'provider_fixture_id':remote['id'],'selection_name':name,'source_market_updated_at':updated,'odds_format':'decimal','settlement_rules_verified':False,'sharp_benchmark_verified':False}})
    return rows

async def collect(sport):
    from laboratory import get,put,sid_for
    key=os.getenv('ODDS_API_KEY');configured=[x.strip() for x in os.getenv('ODDS_API_SPORT_KEYS','').split(',') if x.strip()]
    if not key or not configured: return 0
    sports=await get_json('https://api.the-odds-api.com/v4/sports',{'apiKey':key})
    # Explicit sport keys constrain quota usage. No automatic subscription or credit purchase.
    allowed={r['key'] for r in sports if r.get('active') and (str(r.get('group','')).lower() in ('soccer','tennis')) and ('tennis' if str(r.get('group','')).lower()=='tennis' else 'football')==sport}
    now=clock();sid=await sid_for(sport)
    events=await get('events',sport_id='eq.'+sid,starts_at='gte.'+(now-timedelta(hours=6)).isoformat(),**{'and':'(starts_at.lte.'+(now+timedelta(days=3)).isoformat()+')'},limit=500)
    total=0
    quota_key='daily_requests:'+now.date().isoformat()
    budget=max(0,int(os.getenv('ODDS_API_DAILY_REQUESTS_PER_SPORT','6')))
    checkpoints=await get('backfill_checkpoints',provider='eq.the_odds_api',stream='eq.'+sport,cursor_key='eq.'+quota_key,limit=1)
    used=int(checkpoints[0].get('cursor_value') or 0) if checkpoints else 0
    for skey in configured:
        if skey not in allowed or used>=budget: continue
        used+=1
        await put('backfill_checkpoints',{'provider':'the_odds_api','stream':sport,'cursor_key':quota_key,'cursor_value':str(used),'status':'running','updated_at':clock().isoformat()},'provider,stream,cursor_key',True)
        base=f'https://api.the-odds-api.com/v4/sports/{skey}/odds'
        payload=await get_json(base,{'apiKey':key,'regions':os.getenv('ODDS_API_REGIONS','eu'),'markets':'h2h','oddsFormat':'decimal','dateFormat':'iso'})
        for remote in payload:
            event=match_fixture(remote,events)
            if not event: continue
            observed=clock().isoformat();rows=normalize(remote,event,sport,observed,base)
            if rows: await put('odds_snapshots',rows);total+=len(rows)
            await put('event_snapshots',{'event_id':event['id'],'provider':'the_odds_api_fixture','observed_at':observed,'effective_at':None,'quality':None,'payload':{'provider_fixture_id':remote['id'],'participants':[remote['home_team'],remote['away_team']],'starts_at':remote['commence_time'],'source_url':base,'matching_method':'unique_exact_normalized_pair_within_15_minutes','independent_of_espn_endpoint':True,'upstream_independence_verified':False}})
    return total

def install(ctx):
    CTX.update(ctx)
    @ctx['app'].on_event('startup')
    async def start():
        if collection_paused() or not ctx['SUPABASE_KEY'] or not os.getenv('ODDS_API_KEY') or not os.getenv('ODDS_API_SPORT_KEYS'): return
        from laboratory import tracked,BACKGROUND
        async def lane(sport):
            while True:
                await tracked('bookmaker_odds',sport,lambda:collect(sport))
                await asyncio.sleep(900)
        for sport in ('football','tennis'): BACKGROUND.append(asyncio.create_task(lane(sport)))
