---create groups/ tranches in CSV ("J:\GT-Marketing\Production Processes\Weekly\MDL - Models in Prod\PO Model Tranches\TRANCHE_TAB.csv")
--import groups to a table - mdl_po_analysis_base
/*
drop table mdl_po_analysis_base;
create table mdl_po_analysis_base
(
COL_CD   number,
VAL_CD   number,
TAB   varchar(10),
COL   varchar(25),
COLUMN_TYPE   varchar(1),
OPRT   varchar(5),
VAL   varchar(30),
GRP   varchar(15),
ACTIVE_FLAG varchar(2)
);
commit;
*/
truncate table MDL_PO_ANALYSIS_HIST;

--created base data since 11/15/2017 from MDM_LOAN, MDM_APP and GTS_MONTHLY HISTORY one month before current month
--will require modification in the next 2 insert statements if new columns are added to MDL_PO_ANALYSIS_BASE
insert into MDL_PO_ANALYSIS_HIST
select
t.account_number,
l.servicing_source,
case when l.msr_owned_flag = 'Z' then 'NRZ' else 'Ditech' end as msr,
t.batch_date,
l.MOD_STATUS,
l.INVESTOR_FULL_NAME,
l.PROPERTY_TYPE_CODE,
t.INTEREST_RATE,
l.LOAN_TERM,
l.CURRENT_OCCUPANCY_CODE,
t.UNPAID_BALANCE,
nvl(length(replace(t.delq_12_mth_str,'0','')),0) as DELINQUENCY_COUNT,
l.RATE_TYPE,
l.BANKRUPTCY_CODE,
nvl(t.current_fico_prim,0) as FICO,
cast(to_char(l.origination_date,'yyyy') as number) as ORIGINATION_YEAR,
case when l.payoff_date between dt.start_date and dt.end_date then 1 else 0 end as po,
case when l.payoff_date between dt.start_date and dt.end_date and f.prior_account_no is not null then 1 else 0 end as ret,
null as str
from
(
	select
	batch_date as start_date,
	case when LEAD(batch_date, 1) OVER (ORDER BY batch_date) is null then trunc(sysdate, 'MM') + 14 else LEAD(batch_date, 1) OVER (ORDER BY batch_date) end as end_date
	from
	(
			select batch_date
			from CSRG_PROD.GTS_MTH_HISTORY_T
			where batch_date between '11/1/2017' and trunc(sysdate, 'MM')
			group by batch_date
	)
) dt,
mdm_loan l,
gts_mth_history_t t,
(select distinct prior_account_no from mdm_app where app_fund_date is not null and prior_account_no is not null) f
where l.account_number = t.account_number
and l.account_number = f.prior_account_no(+)
and (l.payoff_date >= '11/15/2017' or l.payoff_date is null)
and t.batch_date  = dt.start_date
and nvl(l.payoff_date,to_date('12/31/2050','mm/dd/yyyy')) > t.batch_date
and l.loan_status_code in ('1', '8')
and l.lien_position = '01'
and l.servicing_source = 'MSP';
commit;

--Add current month from MDM_LOAN for forecasting
insert into MDL_PO_ANALYSIS_HIST
select
ACCOUNT_NUMBER,
SERVICING_SOURCE,
case when msr_owned_flag = 'Z' then 'NRZ' else 'Ditech' end as MSR,
trunc(sysdate) as BATCH_DATE,
MOD_STATUS,
INVESTOR_FULL_NAME,
PROPERTY_TYPE_CODE,
INTEREST_RATE,
LOAN_TERM,
CURRENT_OCCUPANCY_CODE,
UNPAID_BALANCE,
nvl(length(replace(delq_12_mth_str,'0','')),0) as DELINQUENCY_COUNT,
RATE_TYPE,
BANKRUPTCY_CODE,
nvl(current_fico_prim,0) as FICO,
cast(to_char(origination_date,'yyyy') as number) as ORIGINATION_YEAR,
0 as PO,
0 as RET,
null as str
from mdm_loan
where servicing_source = 'MSP'
and lien_position = '01'
and loan_status_code = '1';
commit;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\Data\GT-Marketing\temp\PO_ANALYSIS.SQL
SELECT STR FROM
(
select
2 as seq,
col_cd,
val_cd,
case when col_cd = 1 and val_cd = 1 then 'UPDATE MDL_PO_ANALYSIS_HIST SET STR = (CASE ' ||
			case when column_type = 'C' then 'WHEN ' || COL || OPRT || '''' || VAL || ''' THEN ''' || GRP || ''''
			else 'WHEN ' || COL || OPRT || VAL || ' THEN ''' || GRP || '''' end
     when col_cd = (select max(col_cd) from mdl_po_analysis_base where active_flag = 'Y')
     							and val_cd = (select max(val_cd) from mdl_po_analysis_base where active_flag = 'Y' and col_cd = (select max(col_cd) from mdl_po_analysis_base where active_flag = 'Y')) then
			case when column_type = 'C' then 'WHEN ' || COL || OPRT || '''' || VAL || ''' THEN ''' || GRP || ''''
			else 'WHEN ' || COL || OPRT || VAL || ' THEN ''' || GRP || '''' end ||
			' END); COMMIT;'
     when val_cd <> 1 then
			case when column_type = 'C' then 'WHEN ' || COL || OPRT || '''' || VAL || ''' THEN ''' || GRP || ''''
			else 'WHEN ' || COL || OPRT || VAL || ' THEN ''' || GRP || '''' end
     when col_cd <> 1 and val_cd = 1 then ' END)' || '|| ''-'' || (CASE ' ||
			case when column_type = 'C' then 'WHEN ' || COL || OPRT || '''' || VAL || ''' THEN ''' || GRP || ''''
			else 'WHEN ' || COL || OPRT || VAL || ' THEN ''' || GRP || '''' end
else null end as str
from mdl_po_analysis_base
where active_flag = 'Y'
/*
union all

select distinct
1 as seq,
col_cd,
0 as val_cd,
case when col_cd = 1 then 'INSERT INTO MDL_PO_ANALYSIS_TRANCHE SELECT BATCH_MTH, G' || col_cd || ' || ''-'' ||'
when col_cd = (select max(col_cd) from mdl_po_analysis_base where active_flag = 'Y') then 'G' || col_cd || ' AS STR, COUNT(*) AS CNT, SUM(PO) AS PO, SUM(RET) AS RET, 0 AS INTERCEPT, 0 AS COEFF FROM ('
else 'G' || col_cd || ' || ''-'' ||' end as str
from mdl_po_analysis_base
where active_flag = 'Y'

union all

select distinct
3 as seq,
col_cd,
0 as val_cd,
case when col_cd = 1 then ') GROUP BY BATCH_MTH, G' || col_cd || ' || ''-'' ||'
when col_cd = (select max(col_cd) from mdl_po_analysis_base where active_flag = 'Y') then 'G' || col_cd ||'; COMMIT;'
else 'G' || col_cd || ' || ''-'' ||' end as str
from mdl_po_analysis_base
where active_flag = 'Y'*/
)
ORDER BY seq, col_cd, val_cd;
spool off

START "\\ditech.us\Data\GT-Marketing\temp\PO_ANALYSIS.SQL";

commit;

truncate table mdl_po_analysis_tranche;

insert into mdl_po_analysis_tranche
select
to_char(batch_date,'yyyy-mm') as batch_mth,
str,
count(*) as cnt,
sum(po) as po,
sum(ret) as ret
from mdl_po_analysis_hist
group by
to_char(batch_date,'yyyy-mm'),
str;
commit;


--following 2 codes estimate projected rates based on the summarized data
--linear regression always built on a snapshots 2 months or older. Previous month is used for validation. Current month onwards as forecast
truncate table mdl_po_analysis_estimates;
insert into mdl_po_analysis_estimates
	select
	rank() over(partition by str order by batch_mth) as rnk,
	r.batch_mth,
	r.str,
	r.actual_po_rate,
	0 as est_po_rate
	from
	(
			select distinct a.*,
			round(case when a.batch_mth > to_char(trunc(sysdate, 'MM')-32,'yyyy-mm') then 0 else po end / case when a.batch_mth > to_char(trunc(sysdate, 'MM')-32,'yyyy-mm') then 1 else cnt end * 100,3) as actual_po_rate
			from
			(
					select
					to_char(to_date(mm || '/1' ||'/' || yy,'mm/dd/yyyy'),'yyyy-mm') as batch_mth,
					str
					from
					(select level as yy from dual where level >= 2017 connect by level <= 2020) x,
					(select level as mm from dual connect by level <= 12) y,
					(select distinct str from mdl_po_analysis_tranche) z
					where to_char(to_date(mm || '/1' ||'/' || yy,'mm/dd/yyyy'),'yyyy-mm') >= '2017-11'
			) a,
			mdl_po_analysis_tranche b
			where a.str = b.str
			and (case when a.batch_mth = b.batch_mth then 1 when a.batch_mth > to_char(trunc(sysdate, 'MM')-32,'yyyy-mm') then 1 else 0 end) = 1
	) r;
commit;

merge into mdl_po_analysis_estimates a
using
(
		select distinct
		t.str,
		REGR_INTERCEPT(t.po_rate,t.rnk) over(partition by t.str) as int,
		REGR_SLOPE(t.po_rate, t.rnk) over(partition by t.str) as coeff

		FROM
		(
				select str,
				rank() over(partition by str order by batch_mth) as rnk,
				batch_mth, cnt, po,
				round(po/cnt*100,4) as po_rate
				from MDL_PO_ANALYSIS_TRANCHE a
				where batch_mth <= to_char(trunc(sysdate, 'MM')-32,'yyyy-mm')
		) t
) z
on (a.str = z.str)
when matched then update set
a.est_po_rate = case when nvl(z.int,0) = 0 and nvl(z.coeff,0) =0 then 0 else round(z.int + (z.coeff * rnk) ,4) end;
commit;

--Validating on Prior month

select
decile,
count(*)as cnt,
sum(po) as po,
sum(ret) as ret,
round(sum(po)/count(*)*100,2) as po_rate,
round(sum(ret)/sum(po)*100,2) as recap_rate
from
(
	select
	b.*,
	a.est_po_rate,
	ntile(20) over(order by a.est_po_rate desc, interest_rate, b.unpaid_balance desc, rownum) as decile
	from
	(
	select *
	from mdl_po_analysis_estimates
	where batch_mth = to_char(trunc(sysdate, 'MM')-2,'yyyy-mm')
	) a,
	mdl_po_analysis_hist b
	where a.str = b.str
	and a.batch_mth = to_char(b.batch_date,'yyyy-mm')
	and b.unpaid_balance > 20000 --to be used as needed
)
group by
decile
order by 1;


--Offers for the month
truncate table mdl_po_analysis_offers;
insert into mdl_po_analysis_offers
select c.account_number,
case when c1.rate_type = 'A' and arm_reset_date <= trunc(sysdate) +365 then '01. ARM to Fixed'
when c1.loan_type in ('F', 'V') and bp2.account_number is not null and bp2.new_rate < c1.interest_rate*100 then '02. FHA/VA With Lower Rate'
when c1.loan_type in ('F', 'V') then '03. FHA/VA With Higher Rate'
when pi.account_number is not null then '04. High PI Savings >$100'
when co.excl_17 = '9999-SELECTED' then '05. Cashout With low PI Inc'
when co.excl_17 = '6042-NEW_PI Cap' then '06. Cashout with High PI Inc'
when bp1.account_number is not null and new_pi_savings_mo  >= 50 then '08. Marginal PI Savings ($50-$100)'
when bp1.account_number is not null and new_pi_savings_mo  >= 0 then '09. Low PI Savings ($0-$49)'
else '10. No offer' end as offer
from ce_loans c, --for exclusions
auto_ce_datapull c1,--for
(select account_number, excl_17 from ce_loans where excl_17 in ('6042-NEW_PI Cap', '9999-SELECTED')) co,
(select account_number from ce_loans where excl_6 = '9999-SELECTED') pi,
(select account_number, new_pi_savings_mo from auto_ce_bpo where offer_key = 8006 and new_pi_savings_mo >= 0) bp1,
(select account_number, new_rate from batch_pricing_output where new_product like ('%STRL%') and transaction = 'RATE TERM REFINANCE') bp2
where
excl_21 = '9999-SELECTED'
and c.account_number = c1.account_number
and c.account_number = co.account_number(+)
and c.account_number = pi.account_number(+)
and c.account_number = bp1.account_number(+)
and c.account_number = bp2.account_number(+);
commit;


--counts for the month
select
decile, offer,
count(*)as cnt,
round(sum((case when est_po_rate < 0 then 0 else est_po_rate end)/100),0) as po_est
from
(
	select
	b.*,
	a.est_po_rate,
	ntile(20) over(order by a.est_po_rate desc, interest_rate, b.unpaid_balance desc, rownum) as decile,
	w.offer
	from
	(
	select *
	from mdl_po_analysis_estimates
	where batch_mth = to_char(trunc(sysdate, 'MM'),'yyyy-mm')
	) a,
	mdl_po_analysis_hist b,
	mdl_po_analysis_offers w
	where a.str = b.str
	and a.batch_mth = to_char(b.batch_date,'yyyy-mm')
	and b.account_number = w.account_number
)
group by
decile, offer
order by 1

