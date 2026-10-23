{{ config(
    materialized='incremental',
    unique_key=['ds', 'company_type'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Жалобы по дням и типам компаний (большой банк, финтех, кредитное бюро...): как меняется доля типов, что в выходные.
-- Инкрементальная: первый запуск строит всё, следующие пересчитывают только последние lookback_days дней (сначала удаляют
-- эти дни, потом вставляют заново). 30 дней -- потому что CFPB добавляет жалобы с опозданием.
-- Полная пересборка (после того как DAG cfpb_resync поправил дни старше 30):  dbt build --full-refresh -s dm_type_daily
select
    e.ds,
    e.company_type,
    d.is_weekend,
    count(*)::int                                          as complaints_cnt,
    (count(*) filter (where e.timely is false))::int       as untimely_cnt,
    (count(*) filter (where e.is_relief))::int             as relief_cnt,
    r.fed_funds_rate,
    '{{ invocation_id }}'                                  as _dbt_run_id,
    now()                                                  as _built_at
from {{ ref('int_complaints_enriched') }} e
join {{ ref('dim_calendar') }} d on d.ds = e.ds
left join {{ source('ods', 'ods_fred_dff') }} r on r.ds = e.ds
{% if is_incremental() %}
where e.ds >= (select coalesce(max(ds), date '1900-01-01') from {{ this }}) - {{ var('lookback_days') }}
{% endif %}
group by e.ds, e.company_type, d.is_weekend, r.fed_funds_rate
