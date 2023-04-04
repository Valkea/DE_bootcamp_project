

  create or replace view `compelling-moon-382321`.`de_project_development`.`stg_daily_temperatures`
  OPTIONS()
  as select 
    -- cast(ID as integer) as id,
    cast(Date as date) as date,
    MAX(cast(CodeINSEEregion as integer)) as codeINSEE,
    MAX(Region) as region,
    AVG(TMindegC) as TMindegC,
    AVG(TMaxdegC) as TMaxdegC,
    AVG(TMoydegC) as TMoydegC,

from `compelling-moon-382321`.`de_project_staging`.`daily_temperatures`
group by date

-- dbt build --m <model.sql> --var 'is_test_run: false'
;

