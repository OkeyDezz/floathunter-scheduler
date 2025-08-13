-- Unique index required for CONCURRENT refresh of a materialized view
-- Ref: Postgres requires at least one unique index without WHERE clause

create unique index if not exists ux_liq_mv_item_key
  on public.liquidity_mv (item_key);


