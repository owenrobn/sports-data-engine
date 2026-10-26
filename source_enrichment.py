from runtime_policy import collection_paused
"""Bounded public-source enrichment. Source payloads remain evidence, not inferred facts."""
import asyncio
from datetime import timedelta
from context_feed import clock,date,get_json
CTX={}

async def collect(sport):
    from laboratory import get,put,sid_for,tracked
    sid=await sid_for(sport);now=clock()
    events=await get('events',sport_id='eq.'+sid,starts_at='gte.'+(now-timedelta(days=14)).isoformat(),**{'and':'(starts_at.lte.'+(now+timedelta(days=2)).isoformat()+')'},order='starts_at.desc',limit=250)
    events=[e for e in events if (e.get('metadata') or {}).get('source')=='espn']
    # A source receipt is not an independent second-source verification.
    recent=await get('event_snapshots',provider='eq.espn_summary',observed_at='gte.'+(now-timedelta(hours=6)).isoformat(),select='event_id',limit=1000)
    attempted={r['event_id'] for r in recent}
    events.sort(key=lambda e:(abs((date(e['starts_at'])-now).total_seconds())))
    total=0
    for e in [e for e in events if e['id'] not in attempted][:8]:
        meta=e.get('metadata') or {};league=meta.get('league');pid=meta.get('provider_match_id')
        if not league or not pid: continue
        path='soccer' if sport=='football' else 'tennis'
        base=f'https://site.api.espn.com/apis/site/v2/sports/{path}/{league}/summary'
        if sport=='tennis':
            parent=meta.get('provider_event_id')
            if not parent: continue
            base=f'https://sports.core.api.espn.com/v2/sports/tennis/leagues/{league}/events/{parent}/competitions/{pid}'
        async def one():
            raw=await get_json(base,{'event':pid} if sport=='football' else None)
            if sport=='tennis':
                comp={k:raw[k] for k in ('id','date','status','competitors','venue','surface','indoor','format','situation','type') if k in raw}
                raw={'header':{'competitions':[comp]},'format':raw.get('format')}
            keys=('header','gameInfo','boxscore','rosters','injuries','standings','winprobability','format')
            payload={k:raw[k] for k in keys if k in raw}
            payload.update(source_url=base,provider_event_id=pid,source_updated_at=None,receipt_kind='public_provider_summary',independent_fixture_verification=False)
            if not any(k in raw for k in ('header','gameInfo','boxscore','rosters')): return 0
            await put('event_snapshots',{'event_id':e['id'],'provider':'espn_summary','observed_at':clock().isoformat(),'effective_at':None,'quality':None,'payload':payload})
            return 1
        total+=(await tracked('espn_summary_match',sport,one)) or 0
        await asyncio.sleep(1)
    return total

async def weather(sport):
    from laboratory import get,put,sid_for
    now=clock();sid=await sid_for(sport)
    events=await get('events',select='id,starts_at',sport_id='eq.'+sid,starts_at='gte.'+(now-timedelta(hours=6)).isoformat(),**{'and':'(starts_at.lte.'+(now+timedelta(days=2)).isoformat()+')'},limit=100)
    total=0
    for event in events:
        prior=await get('event_snapshots',event_id='eq.'+event['id'],provider='eq.open_meteo',observed_at='gte.'+(now-timedelta(hours=1)).isoformat(),limit=1)
        if prior: continue
        receipts=await get('event_snapshots',event_id='eq.'+event['id'],provider='eq.espn_summary',order='observed_at.desc',limit=1)
        if not receipts: continue
        p=receipts[0].get('payload') or {}; comps=(p.get('header') or {}).get('competitions') or []
        venue=(p.get('gameInfo') or {}).get('venue') or (comps[0].get('venue') if comps else None) or {}
        coords=venue.get('coordinates') or {};lat=coords.get('latitude');lon=coords.get('longitude')
        if lat is None or lon is None: continue  # Never substitute a guessed city or stadium location.
        base='https://api.open-meteo.com/v1/forecast'
        raw=await get_json(base,{'latitude':lat,'longitude':lon,'current':'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_gusts_10m','hourly':'temperature_2m,precipitation_probability,wind_speed_10m','forecast_days':3,'timezone':'UTC','wind_speed_unit':'ms'})
        await put('event_snapshots',{'event_id':event['id'],'provider':'open_meteo','observed_at':clock().isoformat(),'effective_at':None,'quality':None,'payload':{'source_url':base,'kind':'modelled_weather_and_forecast_not_station_observation','venue':venue,'coordinate_source_receipt':receipts[0]['id'],'weather':raw,'attribution':'Weather data by Open-Meteo.com; CC BY 4.0','indoor_conditions_known':False}})
        total+=1
        if total>=8: break
    return total

def install(ctx):
    CTX.update(ctx)
    @ctx['app'].on_event('startup')
    async def start():
        if collection_paused() or not ctx['SUPABASE_KEY']: return
        from laboratory import BACKGROUND,tracked
        async def lane(sport):
            while True:
                await tracked('espn_summary',sport,lambda:collect(sport))
                await tracked('venue_weather',sport,lambda:weather(sport))
                await asyncio.sleep(900)
        for sport in ('football','tennis'): BACKGROUND.append(asyncio.create_task(lane(sport)))
