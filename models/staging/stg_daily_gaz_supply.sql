{{ config(materialized='view') }}

select * from {{ source('staging', 'daily_gaz_supply') }}
limit 100