{{ config(materialized='view') }}

select * from {{ source('staging', 'daily_ecomix') }}
limit 100