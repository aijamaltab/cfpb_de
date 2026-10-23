-- Витрина: жалобы на банк по кварталам рядом с его размером (активы и депозиты из FDIC).
-- Берутся только кварталы, целиком лежащие в загруженном окне жалоб и уже опубликованные FDIC.
-- Режим: пересборка целиком (delete + insert), повтор безопасен.
-- Запуск: psql -v ods=${PGUSER}_ods -v dm=${PGUSER}_datamart -f sql/24_dm_bank_size.sql
create schema if not exists :"dm";
create table if not exists :"dm".dm_bank_size_quarter (
    quarter_start        date    not null,   -- первый день квартала
    company              text    not null,   -- название компании как в жалобах CFPB
    banks_cnt            int     not null,   -- сколько банков группы учтено
    complaints_cnt       int     not null,
    assets_k_usd         bigint  not null,   -- активы на конец квартала, тыс. USD
    deposits_k_usd       bigint,
    complaints_per_bn_assets   numeric(12,4) not null,  -- жалоб на 1 млрд USD активов
    complaints_per_bn_deposits numeric(12,4),
    primary key (quarter_start, company)
);

begin;
delete from :"dm".dm_bank_size_quarter;
insert into :"dm".dm_bank_size_quarter
with win as (select min(ds) d0, max(ds) d1 from :"ods".ods_complaints),
fin as (
    select date_trunc('quarter', f.report_date)::date qs, m.company_cfpb company,
           count(*) banks_cnt, sum(f.assets_k_usd) assets, sum(f.deposits_k_usd) deposits
    from :"ods".ods_fdic_financials f
    join :"ods".ods_company_bank m on m.cert = f.cert
    where f.assets_k_usd is not null
    group by 1, 2
),
cmp as (
    select date_trunc('quarter', received_at)::date qs, company, count(*) cnt
    from :"ods".ods_complaints
    where company in (select company_cfpb from :"ods".ods_company_bank)
    group by 1, 2
)
select fin.qs, fin.company, fin.banks_cnt::int, coalesce(cmp.cnt, 0)::int, fin.assets, fin.deposits,
       round(coalesce(cmp.cnt, 0) / (fin.assets / 1e6), 4),
       round(coalesce(cmp.cnt, 0) / nullif(fin.deposits / 1e6, 0), 4)
from fin
left join cmp on cmp.qs = fin.qs and cmp.company = fin.company
cross join win
where fin.qs >= win.d0
  and (fin.qs + interval '3 months')::date - 1 <= win.d1;
commit;

\echo === Контроль: сколько строк по кварталам ===
select quarter_start, count(*) companies, sum(complaints_cnt) complaints
from :"dm".dm_bank_size_quarter group by 1 order by 1;

\echo === Топ-10 за последний квартал: жалоб на 1 млрд активов ===
select company, complaints_cnt, round(assets_k_usd / 1e6) assets_bn_usd, complaints_per_bn_assets
from :"dm".dm_bank_size_quarter
where quarter_start = (select max(quarter_start) from :"dm".dm_bank_size_quarter)
order by complaints_per_bn_assets desc limit 10;
