-- Materialized View + VIEW final de liquidez

create materialized view if not exists public.liquidity_mv as
with base as (
  select
    item_key,
    price_buff,
    best_buy_buff,
    (listings_white + listings_csfloat) as total_listings,
    is_stattrak,
    is_souvenir,
    condition,
    doppler_buff_count
  from public.market_joined
)
select
  item_key,
  /* A) Listing Count (25%) */
  case
    when total_listings >= 500 then 25
    when total_listings >= 200 then 20
    when total_listings >= 100 then 15
    when total_listings >= 50  then 10
    else 5
  end as s_listings,

  /* B) Buy/Sell Gap (25%) com validação */
  case
    when price_buff > 0 and best_buy_buff > price_buff * 1.05 then 5
    when price_buff > 0 and (best_buy_buff/price_buff)*100 >= 90 then 25
    when price_buff > 0 and (best_buy_buff/price_buff)*100 >= 80 then 20
    when price_buff > 0 and (best_buy_buff/price_buff)*100 >= 70 then 15
    when price_buff > 0 and (best_buy_buff/price_buff)*100 >= 60 then 10
    else 5
  end as s_gap,

  /* C) Trading Volume (estimado) – 25% */
  case
    when price_buff > 0 then
      case
        when total_listings * ((best_buy_buff/price_buff)*100)/100 >= 1000 then 25
        when total_listings * ((best_buy_buff/price_buff)*100)/100 >= 500  then 20
        when total_listings * ((best_buy_buff/price_buff)*100)/100 >= 200  then 15
        when total_listings * ((best_buy_buff/price_buff)*100)/100 >= 50   then 10
        else 5
      end
    else 5
  end as s_volume,

  /* D) Steam Volume (estimado) – 25% */
  case
    when total_listings >= 500 and price_buff > 0 and (best_buy_buff/price_buff)*100 >= 90 then 25
    when total_listings >= 200 and price_buff > 0 and (best_buy_buff/price_buff)*100 >= 80 then 20
    when total_listings >= 100 and price_buff > 0 and (best_buy_buff/price_buff)*100 >= 70 then 15
    when total_listings >= 50  and price_buff > 0 and (best_buy_buff/price_buff)*100 >= 60 then 10
    else 5
  end as s_steam,

  /* flags para bônus */
  (case when position('doppler' in lower(item_key)) > 0 then 1 else 0 end) as is_doppler,
  (case when condition = 'FN' then 1 else 0 end) as is_fn,
  (case when is_stattrak then 1 else 0 end) as is_st,
  doppler_buff_count
from base;

create index if not exists idx_liq_mv_item_key on public.liquidity_mv (item_key);
create index if not exists idx_liq_mv_is_doppler on public.liquidity_mv (is_doppler);

create or replace view public.liquidity as
select
  item_key,
  least(
    100,
    round(
      case
        when (is_doppler = 1 and is_fn = 1 and is_st = 0 and doppler_buff_count >= 30)
          then (s_listings + s_gap + s_volume + s_steam) * 1.2
        else (s_listings + s_gap + s_volume + s_steam)
      end
    )
  )::int as liquidity_score
from public.liquidity_mv;


