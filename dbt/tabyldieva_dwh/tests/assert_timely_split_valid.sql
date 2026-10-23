-- В срок + с опозданием не может быть больше, чем всего жалоб (у части жалоб timely пусто, поэтому не равенство).
select ds, product, complaints_cnt, timely_cnt, untimely_cnt
from {{ ref('dm_complaints_daily') }}
where timely_cnt + untimely_cnt > complaints_cnt
