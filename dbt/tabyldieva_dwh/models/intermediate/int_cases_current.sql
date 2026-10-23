{{ config(tags=['cdc']) }}

-- Обращение «как сейчас»: сводим два контура.
--   батч (ods_complaints)       -- состояние жалобы на момент последней суточной загрузки DAG;
--   поток (ods_complaint_cases) -- состояние обращения прямо сейчас, его пишет sink-коннектор Debezium.
-- full outer join, а не left: нужны обе крайности.
--   есть только в потоке -- обращение появилось после последней загрузки батча;
--   есть только в батче  -- обращения нет в потоке (удалено в источнике или старше 30 дней: в источник
--                           при старте положили только последние 30 дней).
-- Батч берём только за период, который покрывает поток (от самого раннего до самого позднего обращения,
-- известного обоим контурам), иначе «только в батче» окажется весь архив и все новые дни DAG.
with cdc_window as (
    select min(c.received_at) at time zone 'UTC' as ts_from,
           max(c.received_at) at time zone 'UTC' as ts_to
    from {{ source('cdc', 'ods_complaint_cases') }} c
    join {{ source('ods', 'ods_complaints') }} o on o.complaint_id = c.complaint_id
),
batch as (
    select
        o.complaint_id, o.product, o.company, o.state, o.timely,
        -- та же логика, что cdc/sql/02_seed_source.sql: статус обращения из company_response CFPB
        case o.company_response
            when 'In progress'                     then 'in_progress'
            when 'Closed with explanation'         then 'closed_explanation'
            when 'Closed with monetary relief'     then 'closed_relief'
            when 'Closed with non-monetary relief' then 'closed_relief'
            else 'closed_other'
        end as status
    from {{ source('ods', 'ods_complaints') }} o
    join cdc_window w on o.received_at between w.ts_from and w.ts_to
)
select
    coalesce(c.complaint_id, b.complaint_id) as complaint_id,
    coalesce(c.product, b.product)           as product,
    coalesce(c.company, b.company)           as company,
    coalesce(c.state,   b.state)             as state,
    coalesce(c.status,  b.status)            as status,
    -- откуда взято значение: по этому столбцу видно, кто «главнее» в каждой строке
    case when c.complaint_id is not null then 'cdc' else 'batch' end as source_of_truth,
    c.updated_at                             as changed_at,
    -- батчевое состояние оставляем рядом: по нему видны расхождения
    b.status                                 as batch_status,
    b.product                                as batch_product
from {{ source('cdc', 'ods_complaint_cases') }} c
full outer join batch b on b.complaint_id = c.complaint_id
