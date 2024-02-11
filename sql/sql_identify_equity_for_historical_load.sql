with a as (
select 'BSE' exg, isin_number, case when security_name='NaN' then issuer_name else security_name end as security_name
from sm.equity_list
where status='Active' and bse=true
UNION
select 'NSE' exg, isin_number, case when security_name='NaN' then issuer_name else security_name end as security_name
from sm.equity_list
where status='Active' and nse=true
)
, b as (
select a.*, stat.min_trade_date, stat.max_trade_date
from a
left join sm.AUDIT_EQUITY_HISTORICAL_LOAD_STATUS stat
on a.isin_number = stat.isin_number and a.exg = stat.exchange
where a.isin_number in ('INE737W01013', 'INE587G01015', 'INE714B01016', 'INE801L01010')
)
select * from b
where min_trade_date is null and max_trade_date is null;