{{  config(materialized='table',
    partition_by={
      "field": "date_mix",
      "data_type": "date",
      "granularity": "day"
    }
)}}

with daily_temperatures as (
    select *, 'Temp' as source_type
    from {{ ref('stg_daily_temperatures') }}
),

daily_gaz_supply as (
    select *, 'Gaz' as source_type
    from {{ ref('stg_daily_gaz_supply') }}
),

daily_ecomix as (
    select *, 'Ecomix' as source_type
    from {{ ref('stg_daily_ecomix') }}
)

select
    -- daily_temperatures.date as date_temp,
    -- daily_gaz_supply.date as date_gaz,
    daily_ecomix.date as date_mix,
    -- daily_temperatures.TMindegC,
    daily_temperatures.TMoydegC,
    -- daily_temperatures.TMaxdegC,
    daily_gaz_supply.stock_gaz,
    daily_ecomix.NucleaireMW,
    daily_ecomix.EolienMW,
    daily_ecomix.SolaireMW
from daily_ecomix
-- from daily_temperatures
-- from daily_gaz_supply
    left join daily_gaz_supply
        on daily_ecomix.date = daily_gaz_supply.date
    left join daily_temperatures
        on daily_ecomix.date = daily_temperatures.date
    
-- limit 100