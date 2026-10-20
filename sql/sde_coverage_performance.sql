create or replace view public.sde_sport_coverage with(security_invoker=true) as
with
state_events as materialized (
 select event_id from public.football_match_events
 union
 select event_id from public.tennis_match_events where payload->>'legacy_parser_incomplete' is distinct from 'true'
),
point_events as materialized (
 select distinct event_id from public.tennis_match_events where event_type='point' and payload->>'legacy_parser_incomplete' is distinct from 'true'
),
linked_events as materialized (select distinct event_id from public.market_match_links),
price_events as materialized (select distinct event_id from public.event_market_timeline where market_price is not null),
trade_markets as materialized (select distinct market_id from public.prediction_market_trades),
trade_events as materialized (
 select distinct l.event_id from public.market_match_links l join trade_markets t on t.market_id=l.market_id or t.market_id=l.condition_id
),
book_markets as materialized (select distinct market_id from public.prediction_market_books),
book_events as materialized (
 select distinct l.event_id from public.market_match_links l join book_markets b on b.market_id=l.market_id
)
select s.slug sport,
 count(e.id) canonical_matches,
 count(e.id) filter(where st.event_id is not null) matches_with_states,
 count(e.id) filter(where le.event_id is not null) linked_matches,
 count(e.id) filter(where pe.event_id is not null) matches_with_prices,
 (select count(*) from public.polymarket_market_catalog c where c.sport=s.slug) catalog_markets,
 (select sum(expected_matches) from public.sde_coverage_universes u where u.sport=s.slug) target_matches,
 (select sum(imported_matches) from public.sde_coverage_universes u where u.sport=s.slug) imported_target_matches,
 count(e.id) filter(where tr.event_id is not null) matches_with_trades,
 count(e.id) filter(where be.event_id is not null) matches_with_books,
 count(e.id) filter(where pt.event_id is not null) matches_with_points
from public.sports s
left join public.events e on e.sport_id=s.id and e.metadata->>'source' is distinct from 'polymarket'
left join state_events st on st.event_id=e.id
left join linked_events le on le.event_id=e.id
left join price_events pe on pe.event_id=e.id
left join trade_events tr on tr.event_id=e.id
left join book_events be on be.event_id=e.id
left join point_events pt on pt.event_id=e.id
where s.slug in ('football','tennis') group by s.slug;