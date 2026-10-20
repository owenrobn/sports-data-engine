-- Optional bounded index maintenance. Rebuilds indexes without deleting observations.
-- Run manually; not a startup migration. A lock/statement timeout rolls back the transaction.
begin read write;
set local lock_timeout='1s';
set local statement_timeout='15s';
reindex index public.idx_pm_snapshots_token_time;
commit;
begin read write;
set local lock_timeout='1s';
set local statement_timeout='15s';
reindex index public.idx_pm_snapshots_condition_time;
reindex index public.prediction_market_snapshots_pkey;
commit;
