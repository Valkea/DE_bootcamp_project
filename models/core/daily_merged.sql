{{ config(materialized='table') }}

with daily_temperatures as (
    select *, 'Temp' as source_type
    from {{ ref('stg_daily_temperatures') }}
),

daily_gaz_supply as (
    select *, 'Gaz' as source_type
    from {{ ref('stg_daily_gaz_supply') }}
)

select
    daily_temperatures.ref_datetime as date_temp,
    daily_gaz_supply.ref_datetime as date_gaz,
    daily_temperatures.TMindegC,
    daily_temperatures.TMoydegC,
    daily_temperatures.TMaxdegC,
    daily_gaz_supply.stock_gaz
from daily_temperatures 
    inner join daily_gaz_supply
        on daily_temperatures.ref_datetime = daily_gaz_supply.ref_datetime