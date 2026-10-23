-- Календарь: по строке на день. Витрина dm_complaints_daily обязана ссылаться только на дни из него.
select
    d::date                                   as ds,           -- сама дата
    extract(year  from d)::int                as year,         -- год
    extract(month from d)::int                as month,        -- месяц, 1..12
    extract(isodow from d)::int               as day_of_week,  -- 1 = понедельник ... 7 = воскресенье
    extract(isodow from d)::int >= 6          as is_weekend,   -- суббота и воскресенье
    date_trunc('month', d)::date              as month_start   -- первое число месяца
from generate_series(
        '{{ var("calendar_start") }}'::date,
        '{{ var("calendar_end") }}'::date,
        interval '1 day'
     ) as d
