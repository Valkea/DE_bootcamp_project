

select 
    cast(Date as date) as date,
    AVG(cast(StockfindejourneeGWhPCS0degC as numeric)) as stock_gaz,
    MAX(Source) as source,
    MAX(PITS) as pits,

from `compelling-moon-382321`.`de_project_staging`.`daily_gaz_supply`
group by date

-- dbt build --m <model.sql> --var 'is_test_run: false'
