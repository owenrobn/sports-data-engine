"""Explicit resource policy for the user's free-only deployment."""
import os

def flag(name,default):
    value=os.getenv(name)
    return default if value is None else value.strip().lower() in ('1','true','yes','on')

def policy():
    free=flag('SDE_FREE_MODE',True)
    return {'free_mode':free,'archive_backfill_enabled':flag('SDE_ENABLE_ARCHIVE_BACKFILL',not free),'deep_trade_backfill_enabled':flag('SDE_ENABLE_DEEP_TRADES',not free),'historical_fixture_expansion_enabled':flag('SDE_ENABLE_OLD_FIXTURES',not free),'performance_days':[7,14],'recent_history_fidelity_minutes':60 if free else 1,'history_markets_per_sport_pass':6 if free else None,'history_preserved':True,'continuous_uptime_guaranteed':False,'note':'Free mode prioritizes recent evidence. Existing historical data and on-demand adapters remain available; automatic old-data expansion is paused. Free hosting can sleep and source coverage remains incomplete.'}
