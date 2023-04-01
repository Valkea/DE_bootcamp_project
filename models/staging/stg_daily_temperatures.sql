select 
    -- cast(ID as integer) as id,
    cast(Date as date) as date,
    MAX(cast(CodeINSEEregion as integer)) as codeINSEE,
    MAX(Region) as region,
    AVG(TMindegC) as TMindegC,
    AVG(TMaxdegC) as TMaxdegC,
    AVG(TMoydegC) as TMoydegC,

from {{ source('staging','daily_temperatures') }}
group by date

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}