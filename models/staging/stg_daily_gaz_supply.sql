{{ config(materialized='view') }}

select 
    cast(Date as date) as date,
    AVG(cast(StockfindejourneeGWhPCS0degC as numeric)) as stock_gaz,
    MAX(Source) as source,
    MAX(PITS) as pits,

from {{ source('staging','daily_gaz_supply') }}
group by date

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}