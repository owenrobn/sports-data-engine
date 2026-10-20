"""IMG-RAE v5 input contract. Coverage declarations are not evidence of availability."""
from fastapi.responses import HTMLResponse

# pillar, requirement, applicable sport, type, collection/computation path
REQUIREMENTS = [
('1 · Fundamental skill','Results, opponents and verified participant identities','both','source','ESPN recent results; API Tennis / Sportmonks when configured'),
('1 · Fundamental skill','Glicko-2 rating R, deviation RD and volatility sigma','both','derived','Versioned rating engine with prior state, opponent ratings and result receipts; not implemented'),
('2 · Live stochastic state','Point, game, set, server, serve/return counts and tiebreak state','tennis','source','Point-by-point collector; complete current-match state and source latency still require verification'),
('2 · Live stochastic state','Bayesian serve probability, hold, set and match probability','tennis','derived','Validated Bayesian / Markov computation from complete live state; not implemented'),
('2 · Live stochastic state','Clock, score, shots on target, dangerous attacks, red cards and xG','football','source','ESPN supplies selected scoreboard statistics; missing fields need a licensed live statistics feed'),
('2 · Live stochastic state','Dynamic goal intensity and score-line pressure','football','derived','Calibrated Poisson model using current state; not implemented'),
('3 · Context and physical','Surface, indoor/outdoor, court speed, venue and wind/environment','tennis','source','ESPN competition/summary receipts and coordinate-qualified modelled weather adapter; surface and court-speed coverage still unverified'),
('3 · Context and physical','Venue and current match environment','football','source','ESPN summary venue receipts; Open-Meteo adapter requires explicit venue coordinates; outdoor estimates do not establish indoor conditions'),
('3 · Context and physical','Height, weight, handedness/preferred foot and age','both','source','ESPN profiles/rosters and TheSportsDB fallback; field coverage varies'),
('3 · Context and physical','7/14-day results, performance, minutes, rest and availability','both','source','Recent evidence packages; full minutes and injury coverage need provider access and completeness checks'),
('3 · Context and physical','7-day weighted minutes load, fatigue and surface adjustment','both','derived','Compute only with complete workload and validated coefficients; not implemented'),
('4 · Hazard and isolation','Circuit tier and current-circuit service-break variance','tennis','source/derived','Verified tournament tier plus recent serve/break sample; Challenger/ITF coverage missing'),
('4 · Hazard and isolation','Rivalry/derby evidence and emotional-friction method','football','source/derived','Sourced rivalry classification plus calibrated method; no invented friction score'),
('4 · Hazard and isolation','Hazard index, circuit lock and singles/parlay eligibility','both','policy/derived','Versioned, validated hazard rules; PRD weights are policy proposals, not observations'),
('5 · Capital and execution','Executable decimal odds, complete outcome set and settlement rules','both','source','Synchronized executable-book capture and optional quota-bounded bookmaker adapter; per-match settlement and benchmark verification still required'),
('5 · Capital and execution','Liquidity, order-book depth, spread and slippage','both','source/derived','Venue-specific executable book snapshots and fill receipts; not interchangeable across venues'),
('5 · Capital and execution','Bankroll, open positions and daily exposure','both','private account','Authorized account integration required; never expose account balances through public evidence routes'),
('5 · Capital and execution','Model edge, quarter-Kelly stake and exposure limits','both','policy/derived','Requires calibrated probability, executable odds and private account state; not implemented'),
('LEDGER · Audit','Second-source fixture verification, synchronized asks and sharp benchmark','both','source','Independent fixture receipt and all-outcome quotes at the decision time; incomplete'),
('LEDGER · Audit','Early/pre-kickoff captures, closing line, settlement, Brier and realized PnL','both','derived/account','Timestamped capture schedule, outcomes and private fill ledger; incomplete'),
]

def contract(sport):
    return {'schema_version':'1.0','requirements_source':'IMG-RAE v5 PRD; LEDGER v1.0 audit requirements','sport':sport,'engine_ready':False,'performance_windows_days':[7,14],'maximum_performance_lookback_days':14,'freshness_policy':{'recent_performance':'Only evidence available before the decision cutoff; target match excluded','live_state':'Separate live decision time required; never backdate live observations into pre-match evidence','profiles':'Stable attributes may predate 14 days; retain observed and source-update times','availability':'Recent confirmation required even for an older ongoing absence','ratings':'Prior model state is distinct from recent performance evidence','markets':'Quote age and synchronization must be checked at decision time'},'requirements':[dict(pillar=p,input=i,sport=s,kind=k,path=c,status='coverage_requires_verification' if 'not implemented' not in c and 'missing' not in c and 'incomplete' not in c and 'required' not in c else 'gap') for p,i,s,k,c in REQUIREMENTS if s in ('both',sport)],'note':'This is the full input contract, not a claim that all required feeds or models are operational. Physical context readiness alone cannot establish IMG-RAE readiness.'}

def install(ctx):
    from fastapi import HTTPException
    from laboratory import page,table,esc
    app=ctx['app']
    def checked(sport):
        if sport not in ('football','tennis'): raise HTTPException(422,'Choose football or tennis')
        return contract(sport)
    @app.get('/data/rae/requirements')
    async def requirements(sport:str='tennis'): return checked(sport)
    @app.get('/rae',response_class=HTMLResponse)
    async def overview(sport:str='tennis'):
        c=checked(sport)
        body='<p><a href="?sport=football">Football</a> · <a href="?sport=tennis">Tennis</a> · <a href="/context?sport='+sport+'">Collected match evidence</a> · <a href="/data/rae/requirements?sport='+sport+'">Engine contract JSON</a></p><p>IMG-RAE requires all five pillars. Physical profiles are only one input. Engine ready: <strong>False</strong>. Collection paths below explicitly show incomplete integrations and computations.</p><p>Recent performance uses the 7 or 14 days before the match decision cutoff. Live state and executable prices require their own current decision-time snapshots. Missing observations remain unknown.</p>'
        body+=table(['Pillar','Required input','Type','Collection / computation path','Coverage'],[[esc(r[k]) for k in ('pillar','input','kind','path','status')] for r in c['requirements']])
        return page('IMG-RAE · Full PRD input coverage',body)
