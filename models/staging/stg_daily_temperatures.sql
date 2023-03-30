select 
    cast(ID as integer) as id,
    cast(Date as date) as ref_datetime,
    cast(CodeINSEEregion as integer) as codeINSEE,
    Region as region,
    TMindegC,
    TMaxdegC,
    TMoydegC,

from {{ source('staging','daily_temperatures') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}