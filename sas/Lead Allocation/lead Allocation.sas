%let min_leads = 40; /*Minimum number of leads to to assigned to the LO on round robin basis -- HAS TO BE GREATER THAN 1*/
%let max_dis = 5000; /*Maximum distance from LO zip of property zip for round robin allocation -- HAS TO BE GREATER THAN min_leads*/
%let max_leads = 25;  /*Maximum leads to be assigned to LO from the remainder after round robin*/
%let reassign_dis = 10; /*distance for reassignment*/

PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW='MRDM$101' PATH="RENPU") ;
                                    
                                    CREATE table TAB1 AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
select
	lo.lnoff_cd,
	lo.lnoff_state,
	lo.lnoff_city,
	lo.lnoff_zip,
	leads.account_number,
	leads.zip as prop_zip,
	leads.state as prop_state,
	leads.pi as pi_savings,
	leads.adj_remaining_term,
	leads.ltv as ltv,
	zl.lat_radian as lolat,
	zc.lat_radian as aclat,
	zl.long_radian as lolong,
	zc.long_radian as aclong,
	lo.lnoff_lic_states as lic_state

	from
		(
		select
		lnoff_cd,
		lnoff_name,
		lnoff_city,
		case when (lnoff_home_zip is null or lnoff_cd in ('DBOLTO', 'SAZER', 'AWADE', 'CHRSAN')) then lnoff_zip else lnoff_home_zip end as lnoff_zip,
		lnoff_state,
		lnoff_lic_states
		from csrg_prod.retail_los
		where ASSIGN_LEAD ='Y'
		/*and lnoff_cd in
		(
			'BEYOUN',
			'BQUICK',
			'CRAMSE',
			'DAQUIN',
			'ELSORI',
			'JFLEMI',
			'JPADIL',
			'JVANN',
			'JWADE',
			'LALIBE',
			'MBARBE',
			'MICHGA',
			'PSLIKE',
			'REREIL',
			'RINZUN',
			'SSCARP',
			'STESMI'
			)*/
		) lo,

		(select
		a.account_number,
		l.prop_zip as zip,
		l.prop_state as state,
		b.new_product,
		b.new_pi_savings_mo as pi,
		b.adj_remaining_term,
		round(b.new_loan_amount / b.most_recent_property_value * 100, 1) as ltv
		from csrg_prod.at_apr a, (select * from CSRG_PROD.LOAN_MASTER where PORTFOLIO IN ('ENOTE', 'MOSAIC', 'RESCAP', 'FLAGSTAR', 'NATCITY')) l,
		(select x.*,
		RANK() OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY NEW_PRODUCT desc, NEW_PI_SAVINGS_MO desc) as PI_RANK
			FROM CSRG_PROD.BATCH_PRICING_OUTPUT X
			WHERE TRANSACTION = 'RATE TERM REFINANCE'
			AND NEW_PRODUCT LIKE ('%FIXED%')
			AND PORTFOLIO IN ('MOSAIC', 'RESCAP', 'FLAGSTAR', 'NATCITY', 'ENOTE')) b
		where a.account_number = b.account_number
		and a.account_number = l.account_number
		and b.pi_rank = 1
		) leads,

	csrg_prod.zipcodes zl,
	csrg_prod.zipcodes zc

	where lo.lnoff_zip = zl.zipcode
	and leads.zip = zc.zipcode

);
quit;

proc sql;
create table tab2 as select 
a.*, monotonic() as seq,
case when a.prop_state = trim(s.st1)
or a.prop_state = trim(s.st2)
or a.prop_state = trim(s.st3)
or a.prop_state = trim(s.st4)
or a.prop_state = trim(s.st5)
then 'Y' else 'N' end as lic_check,
round(3963 * arcos((sin(lolat) * sin(aclat))+(cos(lolat) * cos(aclat) * cos(lolong - aclong))),1) as DISTANCE,
round(pi_savings * ltv * adj_remaining_term, .01)/ max(1,round(3963 * arcos((sin(lolat) * sin(aclat))+(cos(lolat) * cos(aclat) * cos(lolong - aclong))),1)) 
as SCORE
from tab1 as a,
(
select lnoff_cd,
left(scan(lic_state,1, ',')) as st1,
left(scan(lic_state,2, ',')) as st2,
left(scan(lic_state,3, ',')) as st3,
left(scan(lic_state,4, ',')) as st4,
left(scan(lic_state,5, ',')) as st5
from
(select distinct lnoff_cd, lic_state from tab1)
) as s
where a.lnoff_cd = s.lnoff_cd
order by a.account_number, distance /*, score*/;
delete from tab2 where lic_check = 'N';
delete from tab2 where distance > &max_dis + 50;
quit;

proc sql;
delete from tab2 where 
(lnoff_cd = 'CCASTR' and prop_state = 'TX') or
(lnoff_cd = 'IGANSS' and prop_state = 'TX') or
(lnoff_cd = 'MBARBE' and prop_state = 'AZ');
quit;


PROC SQL;
CREATE TABLE LO_LIMIT AS SELECT DISTINCT
A.LNOFF_CD,
25 AS MAX_CAP
FROM TAB2 A;
QUIT;


proc sql;
create table tab3 as select a.lnoff_cd, a.no_of_accts, b.max_cap 
from
(select lnoff_cd, count(*) as no_of_accts from tab2 
/*where ((prop_state = 'CA' and distance <= 300) or (distance < &max_dis))*/
group by lnoff_cd
) as a, lo_limit as b
where a.lnoff_cd = b.lnoff_cd
order by no_of_accts;
quit;

proc sql inobs = 2;
create table final_allocation as select * from tab2 quit;
delete * from final_allocation;
quit;

proc sql noprint;
select count(*) 
into : numrows
from (select lnoff_cd from tab3);

select lnoff_cd
into : lncd1- : lncd%sysfunc(trim(&numrows))
from (select lnoff_cd from tab3);

select max_cap
into : cap1- : cap%sysfunc(trim(&numrows))
from (select max_cap from tab3);
quit;


filename junk dummy;
proc printto  log=junk; run;

%macro add_rec;
%local i;
%local c;
%do c = 1 %to &min_leads;
%do i = 1 %to &numrows;
%let x = cap%sysfunc(trim(&i));
%let a = lncd%sysfunc(trim(&i));
%if &c <= &&&x %then %do;
proc sql;
create table m as select x.* from
(select a.* from tab2 as a 
where a.lnoff_cd = "&&&a"
and ((prop_state = 'CA' and distance <= 5000) or (distance < &max_dis))
group by lnoff_cd having a.score = max(a.score)) as x
group by x.lnoff_cd
having (x.seq) = min(x.seq);
quit;
proc append base = final_allocation data = m; run;
proc sql;
delete from tab2 where account_number in (select account_number from m);
quit;
%end;
%end;
%end;
%mend add_rec;
%add_rec

proc printto; run;


proc sql;
create table lead_allocation as select
a.LNOFF_CD,
a.LNOFF_ZIP,
a.LNOFF_STATE,
a.LIC_STATE AS LIC_STATES,
a.ACCOUNT_NUMBER,
a.PROP_ZIP,
a.PROP_STATE,
a.DISTANCE as ZIP_DISTANCE ,
a.pi_savings,
a.ltv,
a.adj_remaining_term,
a.score,
b.max_cap
from
final_allocation as a, lo_limit as b
where a.lnoff_cd = b.lnoff_cd
order by lnoff_cd;
quit;

proc sql;
create table summary as 
select
B.LNOFF_ZIP,
B.LNOFF_CD,
B.LNOFF_STATE,
B.LIC_STATES AS LNOFF_LIC_STATES,
COUNT(*) AS WAVE_4_LEADS,
AVG(MAX_CAP) AS MAX_CAP,
MIN(ZIP_DISTANCE) AS MINIMUM_DISTANCE,
MAX(ZIP_DISTANCE) AS MAXIMUM_DISTANCE,
ROUND(AVG(ZIP_DISTANCE),1) AS AVERAGE_DISTANCE ,
ROUND(AVG(PI_SAVINGS),1) AS AVERAGE_PI_SAVINGS,
ROUND(AVG(LTV),1) AS AVERAGE_LTV,
ROUND(AVG(adj_REMAINING_TERM),1) AS AVERAGE_REMAINING_TERM,
ROUND(AVG(SCORE),1) AS AVERAGE_SCORE
FROM
LEAD_ALLOCATION AS B
GROUP BY 
B.LNOFF_ZIP,
B.LNOFF_CD,
B.LNOFF_STATE,
B.LIC_STATES;
QUIT;

proc sql;
create table rl as select 
ACCOUNT_NUMBER	AS ACCOUNT_NUMBER LENGTH = 45,
PROP_ZIP AS ZIP LENGTH = 30,
PROP_STATE AS STATE LENGTH = 15,
'RETAIL LO HARP AUG 13-A' AS LIST_ID	LENGTH = 69,
LNOFF_CD AS LNOFF_CD LENGTH = 45,
'Y' AS ASSIGNABLE_LEAD LENGTH = 6
FROM LEAD_ALLOCATION;
QUIT;


libname csrg  oracle user=CSRG_PROD password='mrdm$101' path=RENPU schema=CSRG_PROD;
PROC APPEND BASE = CSRG.RETAIL_LEADS DATA = RL; RUN;
libname csrg clear;
