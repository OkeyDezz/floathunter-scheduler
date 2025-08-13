-- Função para atualizar a MV de forma concorrente

create or replace function public.refresh_liquidity_mv()
returns void
language sql
security definer
as $$
  refresh materialized view concurrently public.liquidity_mv;
$$;

revoke all on function public.refresh_liquidity_mv() from public;
grant execute on function public.refresh_liquidity_mv() to authenticated;
grant execute on function public.refresh_liquidity_mv() to service_role;


