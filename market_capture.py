"""Read-only executable book collection; never submits orders."""
import asyncio
from datetime import timedelta
from context_feed import clock,date
CTX={}
async def collect(sport):
    from laboratory import get,put,sid_for,tracked
    now=clock();sid=await sid_for(sport)
    events=await get('events',sport_id='eq.'+sid,starts_at='gte.'+(now-timedelta(hours=6)).isoformat(),**{'and':'(starts_at.lte.'+(now+timedelta(days=1)).isoformat()+')'},order='starts_at.asc',limit=100)
    total=0
    for event in events:
        if (event.get('metadata') or {}).get('status',{}).get('type',{}).get('completed'): continue
        links=await get('market_match_links',event_id='eq.'+event['id'],order='id.asc',limit=10)
        for link in links:
            catalog=await get('polymarket_market_catalog',market_id='eq.'+link['market_id'],closed='eq.false',limit=1)
            if not catalog: continue
            meta=catalog[0].get('metadata') or {};tokens=meta.get('clobTokenIds') or [];outcomes=meta.get('outcomes') or []
            if not tokens or len(tokens)!=len(outcomes): continue
            async def capture():
                before=clock()
                # Independent token calls start together; never assume sequential books are synchronized.
                await asyncio.gather(*(CTX['polymarket_book_snapshot'](str(t),event['id'],link['market_id']) for t in tokens))
                after=clock();start=date(event.get('starts_at'));seconds=(start-before).total_seconds() if start else None
                phase='scheduled_pre_10m' if seconds is not None and 0<=seconds<=600 else 'early' if seconds is not None and seconds>600 else 'after_scheduled_start'
                await put('event_snapshots',{'event_id':event['id'],'provider':'clob_capture','observed_at':after.isoformat(),'effective_at':None,'quality':None,'payload':{'market_id':link['market_id'],'tokens':tokens,'outcomes':outcomes,'mapping_observed_at':catalog[0]['observed_at'],'capture_started_at':before.isoformat(),'capture_finished_at':after.isoformat(),'phase':phase,'scheduled_start':event['starts_at'],'actual_kickoff_verified':False,'source_url':'https://clob.polymarket.com/book','orders_submitted':False}})
                return len(tokens)
            total+=await tracked('clob_capture_market',sport,capture)
        if total>=40: break
    return total

def install(ctx):
    CTX.update(ctx)
    @ctx['app'].on_event('startup')
    async def start():
        if not ctx['SUPABASE_KEY']: return
        from laboratory import tracked,BACKGROUND
        async def lane(sport):
            while True:
                await tracked('executable_books',sport,lambda:collect(sport))
                await asyncio.sleep(60)
        for sport in ('football','tennis'): BACKGROUND.append(asyncio.create_task(lane(sport)))
