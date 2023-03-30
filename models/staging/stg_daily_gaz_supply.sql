-- {{ config(materialized='view') }}
-- 
-- select * from {{ source('staging', 'daily_gaz_supply') }}
-- limit 100

{{ config(materialized='view') }}

-- with tripdata as 
-- (
--     select *,
--     row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
--     from {{ source('staging','daily_gaz_supply') }}
--     where vendorid is not null 
-- )

select 
    cast(Date as date) as ref_datetime,
    cast(StockfindejourneeGWhPCS0degC as numeric) as stock_gaz,
    Source as source,
    PITS as pits,

from {{ source('staging','daily_gaz_supply') }}
-- from tripdata
-- where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}