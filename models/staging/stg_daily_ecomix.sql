{{ config(materialized='view') }}

select 
    MAX(Perimetre) as Perimetre,
    MAX(Nature) as Nature,
    cast(Date as date) as date,
    -- cast(Heure as hour) as hour,
    -- cast(DateHeure as hour) as datetime,
    AVG(cast(ConsommationMW as numeric)) as ConsommationMW,
    AVG(cast(PrevisionJ1MW as numeric))as PrevisionJ1MW,
    AVG(cast(PrevisionJMW as numeric)) as PrevisionJMW,
    AVG(cast(FioulMW as numeric)) FioulMW,
    AVG(cast(CharbonMW as numeric)) as CharbonMW,
    AVG(cast(GazMW as numeric)) as GazMW,
    AVG(cast(NucleaireMW as numeric)) as NucleaireMW,
    AVG(cast(EolienMW as numeric)) as EolienMW,
    AVG(cast(SolaireMW as numeric)) as SolaireMW,
    AVG(cast(HydrauliqueMW as numeric)) as HydrauliqueMW,
    AVG(cast(PompageMW as numeric)) as PompageMW,
    AVG(cast(BioenergiesMW as numeric)) as BioenergiesMW,
    AVG(cast(EchphysiquesMW as numeric)) as EchphysiquesMW,
    AVG(cast(TauxdeCO2gkWh as numeric)) as TauxdeCO2gkWh,
    AVG(cast(EchcommAngleterreMW as numeric)) as EchcommAngleterreMW,
    AVG(cast(EchcommEspagneMW as numeric)) as EchcommEspagneMW,
    AVG(cast(EchcommItalieMW as numeric)) as EchcommItalieMW,
    AVG(cast(EchcommSuisseMW as numeric)) as EchcommSuisseMW,
    AVG(cast(EchcommAllemagneBelgiqueMW as numeric)) as EchcommAllemagneBelgiqueMW,
    AVG(cast(FioulTACMW as numeric)) as FioulTACMW,
    AVG(cast(FioulCogenerationMW as numeric)) as FioulCogenerationMW,
    AVG(cast(FioulAutresMW as numeric)) as FioulAutresMW,
    AVG(cast(GazTACMW as numeric)) as GazTACMW,
    AVG(cast(GazCogenerationMW as numeric)) as GazCogenerationMW,
    AVG(cast(GazCCGMW as numeric)) as GazCCGMW,
    AVG(cast(GazAutresMW as numeric)) as GazAutresMW,
    AVG(cast(HydrauliqueFildeleauecluseeMW as numeric)) as HydrauliqueFildeleauecluseeMW,
    AVG(cast(HydrauliqueLacsMW as numeric)) as HydrauliqueLacsMW,
    AVG(cast(HydrauliqueSTEPturbinageMW as numeric)) as HydrauliqueSTEPturbinageMW,
    AVG(cast(BioenergiesDechetsMW as numeric)) as BioenergiesDechetsMW,
    AVG(cast(BioenergiesBiomasseMW as numeric)) as BioenergiesBiomasseMW,
    AVG(cast(BioenergiesBiogazMW as numeric)) as BioenergiesBiogazMW,
    AVG(cast(StockagebatterieMW as numeric)) as StockagebatterieMW,
    AVG(cast(DestockagebatterieMW as numeric)) as DestockagebatterieMW,
    AVG(cast(EolienterrestreMW as numeric)) as EolienterrestreMW,
    AVG(cast(EolienoffshoreMW as numeric)) as EolienoffshoreMW,

from {{ source('staging','daily_ecomix') }}
group by date

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}