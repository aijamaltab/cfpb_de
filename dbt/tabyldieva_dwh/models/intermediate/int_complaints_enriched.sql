-- Жалоба с обогащением: одна строка = одна жалоба (ключ complaint_id), без потери строк.
--   тип компании берётся из справочника-CSV (ods_dim_company); нет в справочнике -> 'other';
--   название штата -- из seed us_states; флаги «в работе» и «компенсация» считаются один раз здесь,
--   чтобы три витрины не повторяли одни и те же условия.
-- left join, а не inner: справочники неполные, а жалобу терять нельзя.
-- Справочники уникальны по ключу (тесты на источниках), поэтому строки не размножаются.
-- Штат: для Малых отдалённых островов США CFPB присылает не код, а полное название ('UNITED STATES MINOR OUTLYING ISLANDS',
-- 48 жалоб на 855 110). Приводим к коду UM до присоединения справочника, иначе строка уходит в «не определён».
with src as (
    select
        o.*,
        case when o.state = 'UNITED STATES MINOR OUTLYING ISLANDS' then 'UM' else o.state end as state_code_norm
    from {{ source('ods', 'ods_complaints') }} o
)
select
    o.complaint_id,
    o.ds,
    date_trunc('month', o.ds)::date                          as month_start,
    o.product,
    o.company,
    coalesce(c.company_type, 'other')                        as company_type,
    coalesce(nullif(o.state_code_norm, ''), 'unknown')       as state,
    coalesce(s.state_name, 'не определён')                   as state_name,
    o.timely,
    o.company_response,
    (o.company_response = 'In progress')                     as is_in_progress,
    (o.company_response in ('Closed with monetary relief',
                            'Closed with non-monetary relief')) as is_relief
from src o
left join {{ source('ods', 'ods_dim_company') }} c on c.company_cfpb = o.company
left join {{ ref('us_states') }}                 s on s.state_code   = o.state_code_norm
