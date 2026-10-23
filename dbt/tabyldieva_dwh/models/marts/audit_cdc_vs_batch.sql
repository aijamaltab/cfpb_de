{{ config(tags=['cdc']) }}

-- Где батч (DAG) и поток (CDC) разошлись. Это не ошибка, а нормальная работа двух контуров с разной скоростью:
-- пока DAG не загрузил день заново, он не знает, что обращение закрыли. Витрина нужна, чтобы расхождение
-- было ВИДНО. Пустая витрина = контуры сошлись.
select
    complaint_id,
    case
        when batch_status is null and source_of_truth = 'cdc'
            then 'только в потоке: обращение появилось после последней загрузки'
        when source_of_truth = 'batch'
            then 'только в батче: в источнике строки уже нет (удалена)'
        else 'статус разошёлся: обращение изменили после последней загрузки'
    end                     as problem,
    batch_status,           -- что знает батч
    status as cdc_status,   -- что знает поток
    changed_at,             -- когда источник изменился
    now()  as _built_at
from {{ ref('int_cases_current') }}
where batch_status is null                    -- нет в батче
   or source_of_truth = 'batch'               -- нет в потоке
   or status is distinct from batch_status    -- разный статус
