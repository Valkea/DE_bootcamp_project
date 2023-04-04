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

    -- REF DATE

    daily_ecomix.date as date_mix,
        -- daily_temperatures.date as date_temp,
        -- daily_gaz_supply.date as date_gaz,

    -- TEMPERATURES

    daily_temperatures.TMoydegC,
        -- daily_temperatures.TMindegC,
        -- daily_temperatures.TMaxdegC,

    CASE
        WHEN daily_temperatures.TMoydegC <= 10.0 THEN 'cold'
        WHEN daily_temperatures.TMoydegC > 25.0 THEN 'hot'
        ELSE 'average'
        END AS temperature_category,

    -- CO2 PRODUCTION

    daily_ecomix.TauxdeCO2gkWh,
    
    -- ENERGY SOURCES

    daily_ecomix.FioulMW,
    daily_ecomix.CharbonMW,
    daily_ecomix.GazMW,
    daily_ecomix.NucleaireMW,
    daily_ecomix.EolienMW,
    daily_ecomix.SolaireMW,
    daily_ecomix.HydrauliqueMW,
    daily_ecomix.PompageMW,
    daily_ecomix.BioenergiesMW,

    CASE greatest(daily_ecomix.NucleaireMW, daily_ecomix.EolienMW, daily_ecomix.SolaireMW)
        WHEN daily_ecomix.NucleaireMW THEN 'nucleaire'
        WHEN daily_ecomix.EolienMW    THEN 'eolien'
        WHEN daily_ecomix.SolaireMW   THEN 'solaire'
        END AS top_source,

    -- TRADING DETAILS

    daily_ecomix.EchcommAngleterreMW,
    daily_ecomix.EchcommEspagneMW,
    daily_ecomix.EchcommItalieMW,
    daily_ecomix.EchcommSuisseMW,
    daily_ecomix.EchcommAllemagneBelgiqueMW,
    daily_ecomix.EchphysiquesMW,

    CASE greatest(  daily_ecomix.EchcommAngleterreMW,
                    daily_ecomix.EchcommEspagneMW, 
                    daily_ecomix.EchcommItalieMW, 
                    daily_ecomix.EchcommSuisseMW,
                    daily_ecomix.EchcommAllemagneBelgiqueMW,
                    daily_ecomix.EchphysiquesMW
                    )
        WHEN daily_ecomix.EchcommAngleterreMW   THEN 'United Kingdom'
        WHEN daily_ecomix.EchcommEspagneMW      THEN 'Spain'
        WHEN daily_ecomix.EchcommItalieMW       THEN 'Italy'
        WHEN daily_ecomix.EchcommSuisseMW       THEN 'Switzerland'
        WHEN daily_ecomix.EchcommAllemagneBelgiqueMW   THEN 'Germany & Belgium'
        WHEN daily_ecomix.EchphysiquesMW        THEN 'Other'
        END AS top_foreign_buyer,

    -- FIOUL DETAILS

    daily_ecomix.FioulTACMW,
    daily_ecomix.FioulCogenerationMW,
    daily_ecomix.FioulAutresMW,

    -- GAS DETAILS

    daily_ecomix.GazTACMW,
    daily_ecomix.GazCogenerationMW,
    daily_ecomix.GazCCGMW,
    daily_ecomix.GazAutresMW,
    daily_gaz_supply.stock_gaz,

    -- HYDRO DETAILS

    daily_ecomix.HydrauliqueFildeleauecluseeMW,
    daily_ecomix.HydrauliqueLacsMW,
    daily_ecomix.HydrauliqueSTEPturbinageMW,

    -- BIO DETAILS

    daily_ecomix.BioenergiesDechetsMW,
    daily_ecomix.BioenergiesBiomasseMW,
    daily_ecomix.BioenergiesBiogazMW,

    -- WIND DETAILS

    daily_ecomix.EolienterrestreMW,
    daily_ecomix.EolienoffshoreMW,

    -- STORAGE DETAILS

    daily_ecomix.StockagebatterieMW,
    daily_ecomix.DestockagebatterieMW

from daily_ecomix
    left join daily_gaz_supply
        on daily_ecomix.date = daily_gaz_supply.date
    left join daily_temperatures
        on daily_ecomix.date = daily_temperatures.date
    
-- limit 100