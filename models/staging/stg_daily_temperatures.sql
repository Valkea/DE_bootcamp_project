{{ config(materialized='view') }}

select * from {{ source('staging', 'daily_temperatures') }}
limit 100