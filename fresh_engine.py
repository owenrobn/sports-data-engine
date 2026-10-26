"""Read-only IMG-RAE and LEDGER envelopes backed by verified cloud snapshots."""
from fastapi import HTTPException
from context_feed import clock,date

def find(event_id,datasets=None):
    from fresh_feed import output
    for sport in ('football','tennis'):
        data=datasets[sport] if datasets is not None else output(sport)
        target=next((m for m in data['matches'] if m['id']==str(event_id)),None)
        if target: return sport,data,target
    raise HTTPException(404,'Match not collected in the cloud evidence feed')

def envelope(event_id,as_of=None,datasets=None):
    from fresh_feed import match_package
    from fresh_markets import assess
    from engine_delivery import live_fields
    from rae_requirements import contract
    decision=date(as_of) if as_of else clock()
    if not decision or decision>clock(): raise HTTPException(422,'Use a timezone-aware decision time no later than now')
    sport,data,target=find(event_id,datasets)
    context=match_package(str(event_id),decision,data)
    observed=date(target.get('observed_at'));live=target if observed and observed<=decision else None
    state={**live,'participants':[{**p,'provider_id':p['source_id']} for p in live['participants']]} if live else None
    lf=live_fields(sport,state)
    age=(decision-observed).total_seconds() if live else None
    fresh=age is not None and 0<=age<=120
    markets=[assess(r,decision) for r in data['market_receipts'].get(str(event_id),{}).values() if date(r.get('observed_at')) and date(r['observed_at'])<=decision]
    blockers=['independent_fixture_verification','sharp_benchmark_and_settlement_verification','private_account_integration','complete_ledger_capture_and_settlement_history','calibrated_model_and_validated_policy']
    for p in context['participants']:
        if not (p.get('profiles') or {}).get('records'): blockers.append('verified_profiles:'+p['source_id'])
        blockers+=['fresh_availability:'+p['source_id'],'complete_minutes:'+p['source_id']]
    blockers+=['live.'+k for k in lf['missing_fields']]
    if not fresh: blockers.append('fresh_live_snapshot')
    if not markets: blockers.append('verified_market_links_and_executable_books')
    elif not any(m['executable_snapshot_usable'] for m in markets): blockers.append('fresh_complete_synchronized_books')
    if not data['archive'].get('verified'): blockers.append('verified_cloud_persistence')
    return {'schema_version':'2.1','event_id':str(event_id),'match':target['name'],'sport':sport,'decision_time':decision.isoformat(),'pre_match_cutoff':context['pre_match_cutoff'],'engine_ready':False,'readiness_scope':'Source evidence; no prediction or trade authorization','storage_mode':data['collection_mode'],'archive':data['archive'],'blockers':sorted(set(blockers)),'recent_performance':context,'live_state':{'snapshot':live,'retrieval_fresh':fresh,'retrieval_age_seconds':age,'provider_latency_known':False},'normalized_live_inputs':lf,'pre_match_source_summary':context.get('detail_receipt'),'markets':markets,'bookmaker_complete_sets':[],'private_account_data':None,'requirements':contract(sport),'capture_policy':{'performance_days':[7,14],'quote_and_live_retrieval_age_seconds':120,'outcome_synchronization_seconds':5,'actual_kickoff_verified':False,'historical_replay':'Only currently loaded receipts observed by the decision time are returned; immutable older archive snapshots are not automatically replayed.'}}

def ledger(event_id,as_of=None,private=False,datasets=None):
    from fresh_feed import STATE
    from fresh_markets import assess
    from ledger_audit import capture_audit
    p=envelope(event_id,as_of,datasets);decision=date(p['decision_time']);sport=p['sport']
    _,data,target=find(event_id,datasets);captures=[];books=[]
    for receipt in data.get('market_history',[]):
        if receipt.get('event_id')!=str(event_id) or date(receipt['observed_at'])>decision: continue
        start=date(target.get('starts_at'));finish=date(receipt['capture_finished_at']);seconds=(start-finish).total_seconds() if start else None
        phase='scheduled_pre_10m' if seconds is not None and 0<=seconds<=600 else 'early' if seconds is not None and seconds>600 else 'after_scheduled_start'
        # Reuse the audit contract, but do not promote source-stale books to usable.
        rid=receipt['market_id']+':'+receipt['observed_at']
        captures.append({'id':rid,'observed_at':receipt['observed_at'],'payload':{**receipt,'phase':phase}})
        books.extend({**b,'market_id':receipt['market_id']} for b in receipt['books'])
    audit=capture_audit(captures,books,date(target.get('starts_at')),decision)
    return {'event_id':str(event_id),'match':p['match'],'sport':sport,'audit_as_of':decision.isoformat(),'scheduled_start':target.get('starts_at'),'ledger_complete':False,'storage_mode':p['storage_mode'],'archive':p['archive'],'capture_audit':audit,'current_markets':p['markets'],'resolution_receipts':[],'actual_finish_time':None,'result_within_24h_verified':False,'realized_pnl':None,'account_integration':'not_configured','private_forecasts_included':False,'limitations':['Audit covers the bounded currently loaded cloud receipts; earlier immutable snapshots have not been replayed.','Venue settlement, benchmark, actual start, private forecasts and fills remain unverified.']}


async def replay(event_id,as_of,kind='engine',private=False):
    import asyncio
    from datetime import timedelta
    from fresh_feed import hydrate,output
    decision=date(as_of)
    if not decision or decision>clock() or decision<clock()-timedelta(days=14): raise HTTPException(422,'Archive replay requires an aware decision time within the previous 14 days')
    async def one(sport):
        try: snapshot=await asyncio.to_thread(hydrate,sport,decision)
        except Exception as ex: raise HTTPException(503,'Verified cloud archive replay unavailable: '+type(ex).__name__)
        snapshot=snapshot or {'packs':{},'last_poll_at':None,'archived_at':None,'markets':{},'profiles':{},'details':{}}
        state={**snapshot,'errors':[],'archive':{'verified':bool(snapshot.get('archived_at')),'status':'verified_historical_snapshot' if snapshot.get('archived_at') else 'no_snapshot_before_decision','archived_at':snapshot.get('archived_at')}}
        return sport,output(sport,state)
    datasets=dict(await asyncio.gather(one('football'),one('tennis')))
    result=envelope(event_id,as_of,datasets) if kind=='engine' else ledger(event_id,as_of,private,datasets)
    result['historical_replay']={'mode':'verified_immutable_cloud_snapshot','decision_time':decision.isoformat(),'snapshots':{s:d['archive'] for s,d in datasets.items()},'historical_database_included':False}
    return result
