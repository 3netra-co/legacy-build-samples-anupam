                /*-----------------------------EVERREADY BATCH PRICING------------------------------------*/
                /*-----------------------------DEVELOPED BY ANUPAM TRIPATHI-------------------------------*/
                /*-----------------------------VERSION DATE - Jun 6 2011---------------------------------*/

/* TABLE TO CREATE FIRST LIEN EXCLUSIONS - ADDED ON 05 24 2011
- EXCLUDES FIRST LIENS THAT ARE
		DELINQUENT
		HAVE HAD AN APP IN THE LAST SIX MONTHS
		CURRENT PROPERTY VALUE  IS NULL
		STANDARD EXCLUSION = 'Y'
		NOT GMAC OWNED
		NOT ACTIVE
		ORIGINATION DATE IS WITHIN LAST SIX MONTHS
		DO NOT MAIL FLAG IS Y
		UNPAID BALANCE < 75000
		REMAINING TERM < 60
*/
PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW='MRDM$101' PATH="RENPU") ;
                                    
                                    CREATE table EXCLUSIONS AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(

SELECT
ACCOUNT_NUMBER,
LIEN_POSITION,

CASE WHEN STATUS='INACTIVE'
	or DELINQUENCY_12_MONTH_COUNT>2
	or DELINQUENCY_DAYS_PAST_DUE>20
	or (STD_EXCLUSION_FM_FLAG='Y' and POOL_RESTRICTION<>'Restricted Pool')
	or (NEW_APP_NO is not null and NEW_APP_STATUS<>'Q')
	or DNS_MAIL_FLAG in ('Y','E')
	or CURRENT_PROPERTY_VALUE = 0
	or ORIGINATION_DATE > SYSDATE - 180
	or UNPAID_BALANCE < 75000
	or REMAINING_TERM < 60
	THEN 'Not Marketable'
	WHEN DNS_PHONE_FLAG in ('Y','E') then 'Marketable'
	ELSE 'Marketable' end as MARKETABLE,

case when STATUS='INACTIVE'  then 1 else 0 end
	+ case when DELINQUENCY_12_MONTH_COUNT>2 then 1 else 0 end
	+ case when DELINQUENCY_DAYS_PAST_DUE>20  then 1 else 0 end
	+ case when (STD_EXCLUSION_FM_FLAG='Y' and POOL_RESTRICTION<>'Restricted Pool') then 1 else 0 end 
	+ case when (NEW_APP_NO is not null and NEW_APP_STATUS<>'Q') then 1 else 0 end
	+ case when DNS_MAIL_FLAG ='Y'  then 1 else 0 end
	as NUM_REASONS_NM,

CASE WHEN STATUS='INACTIVE' then 1 else 0 end as NM_INACTIVE,
CASE WHEN DELINQUENCY_12_MONTH_COUNT>2 then 1 else 0 end as NM_G2_DQ_PMTS,
CASE WHEN DELINQUENCY_DAYS_PAST_DUE>20 then 1 else 0 end as NM_G20_PAST_DUE,
CASE WHEN (STD_EXCLUSION_FM_FLAG='Y' and POOL_RESTRICTION<>'Restricted Pool')  then 1 else 0 end as NM_STD_EXCL_EXCEPT_RP, 
CASE WHEN (NEW_APP_NO is not null and NEW_APP_STATUS<>'Q')  then 1 else 0 end as NM_ACTIVE_APP,
CASE WHEN DNS_MAIL_FLAG in ('Y','E') then 1 else 0 end as NM_DNS_MAIL,
CASE WHEN CURRENT_PROPERTY_VALUE = 0 THEN 1 ELSE 0 END AS NM_MISSING_AVM,
CASE WHEN ORIGINATION_DATE > SYSDATE - 180 THEN 1 ELSE 0 END AS NM_ORG_WITHIN_6_MTHS

FROM (
		SELECT
		L.ACCOUNT_NUMBER,
		l.LIEN_POSITION,
/*		C.FIRST_NAME  as PRIM_FIRST_NAME, C.MIDDLE_NAME  as PRIM_MIDDLE_NAME, C.LAST_NAME  as PRIM_LAST_NAME, C.NAME_SUFFIX  as PRIM_NAME_SUFFIX,
		C2.FIRST_NAME as SECD_FIRST_NAME, C2.MIDDLE_NAME as SECD_MIDDLE_NAME, C2.LAST_NAME as SECD_LAST_NAME, C2.NAME_SUFFIX as SECD_NAME_SUFFIX,
		SUBSTR(C.HOME_PHONE,-10) as HOME_PHONE,
		SUBSTR(C.WORK_PHONE,-10) as WORK_PHONE,
		LA.ADDRESS_ID as MAIL_ADDRESS_ID,
		LA.ADDRESS_LINE_1 AS MAIL_ADDRESS1,
		LA.ADDRESS_LINE_2 AS MAIL_ADDRESS2,
		LA.CITY AS MAIL_CITY,
		LA.STATE AS MAIL_STATE,
		LA.ZIP AS MAIL_ZIP,

		PA.ADDRESS_ID as PROPERTY_ADDRESS_ID,
		PA.ADDRESS_LINE_1 AS PROPERTY_ADDRESS1,
		PA.ADDRESS_LINE_2 AS PROPERTY_ADDRESS2,
		PA.CITY AS PROPERTY_CITY,
		PA.STATE AS PROPERTY_STATE,
		PA.ZIP AS PROPERTY_ZIP,
*/
		case when L.LOAN_STATUS_CODE not in ('1','9') then 'INACTIVE' else 'ACTIVE' end as STATUS,
		L.STD_EXCLUSION_FM_FLAG,
		LCSC.USER_DEFINED_47 as POOL,
		case when 	to_Number(regexp_substr(LCSC.USER_DEFINED_47,'^[-]?[[:digit:]]*\.?[[:digit:]]*$')) between 1 and 3999
				or  to_Number(regexp_substr(LCSC.USER_DEFINED_47,'^[-]?[[:digit:]]*\.?[[:digit:]]*$')) between 5000 and 39999
				or  to_Number(regexp_substr(LCSC.USER_DEFINED_47,'^[-]?[[:digit:]]*\.?[[:digit:]]*$')) between 50000 and 99999
				then 'Restricted Pool' else null end as POOL_RESTRICTION,
		L.DELINQUENCY_12_MONTH_COUNT,
		L.DELINQUENCY_DAYS_PAST_DUE,
		L.GMAC_OWNED_FLAG,
		L.BANKRUPTCY_CODE,
		L.FORECLOSURE_FLAG,
		L.DNS_MAIL_FLAG,
		L.DNS_PHONE_FLAG,
		L.DNS_EMAIL_FLAG,
		P.CURRENT_PROPERTY_VALUE,
		L.ORIGINATION_DATE,
		L.UNPAID_BALANCE,
		L.REMAINING_TERM,
		( SELECT CASE WHEN	TASKS.date_hmp_mod_approved  IS NOT NULL
				AND TASKS.DATE_MOD_FINAL_COMPLETION  >=  TASKS.date_hmp_mod_approved
				AND (TASKS.DATE_LSMIT_MOD_APRV_INVESTOR  IS NULL
						OR   TASKS.DATE_LSMIT_MOD_APRV_INVESTOR  <  TASKS.date_hmp_mod_approved)
				AND  TASKS.date_hmp_trial_failed  IS NULL
				AND  (TASKS.date_hmp_trial_denied  IS NULL
						OR   TASKS.date_hmp_trial_denied  <  TASKS.date_hmp_mod_approved)
			    THEN 'HMP'
			WHEN TASKS.DATE_LSMIT_MOD_APRV_INVESTOR  IS NOT NULL
		     	OR TASKS.DATE_MOD_FINAL_COMPLETION  >=  TASKS.DATE_LSMIT_MOD_APRV_INVESTOR
		        THEN 'TRAD'
		 	ELSE NULL END
		  FROM GMAC.U_LSMIT_TASKS@GMINFP TASKS
		  WHERE L.ACCOUNT_NUMBER=TASKS.ACCOUNT_NUMBER and TASKS.DATE_MOD_FINAL_COMPLETION is not null) AS MOD_FLAG,

		/*
		EB.DATE_STMP as SSN_NEW_APP_DATE,
		EB.DATE_STMP as SSN_NEW_APP_STATUS,*/
		EL.LOAN_NO as NEW_APP_NO,
		EL.CREATE_DT as NEW_APP_DATE,
		EL.CLIENT_STATUS as NEW_APP_STATUS,
		EL.P_NUMB||' '||EL.P_STREET as NEW_APP_STREET,
		EL.P_ZIP as NEW_APP_ZIP,

		RANK() OVER (PARTITION BY L.ACCOUNT_NUMBER ORDER BY EL.CREATE_DT desc, rownum) as RANK


		FROM ECR_CORE.LOAN@GMEDAPP L,
		ECR_CORE.CUSTOMER@GMEDAPP C,
		ECR_CORE.CUSTOMER@GMEDAPP C2,
		ECR_CORE.PROPERTY@GMEDAPP P,
		ECR_CORE.ADDRESS@GMEDAPP PA,
		ECR_CORE.ADDRESS@GMEDAPP LA,
		PUB.PT_T_LOANSADDL@DTRPTP ELA,
		PUB.LOANS@DTRPTP EL,
		INFORMENT.T_LOAN_CODES_STATUS_CLASS@GMINFP LCSC



		WHERE
		L.PRIMARY_CUSTOMER_ID=C.CUSTOMER_ID
		and L.SECONDARY_CUSTOMER_ID=C2.CUSTOMER_ID(+)
		and L.PROPERTY_ID=P.PROPERTY_ID(+)
		and L.LOAN_ADDRESS_ID=LA.ADDRESS_ID(+)
		and P.ADDRESS_ID=PA.ADDRESS_ID(+)
		and L.ACCOUNT_NUMBER=ELA.SERVICE_ACCT_NO(+)
		and ELA.LOAN_NO=EL.LOAN_NO(+)
		and EL.CREATE_DT(+)> to_date('10/1/2010','mm/dd/yyyy')
		and L.ACCOUNT_NUMBER=LCSC.ACCOUNT_NUMBER(+)
		and L.ACCOUNT_NUMBER in
		(
		select a.account_number from ecr_core.loan@gmedapp a
		where gmac_owned_flag = 'Y'
		and lien_position = '01'
		and loan_status_code in ('1','9')
		        )
ORDER BY 1 )
WHERE RANK=1

);
DISCONNECT FROM CONN1;
QUIT;


%let ufmip = 1.0100; /*FHA Upfront Insurance Premium*/
%let fees_fixed = 1200; /*Fixed loans origination fee*/
%let fees_arm = 1200; /*ARM loans origination fee*/
%let fees_jumbo = 3000; /*Jumbo loans origination fee*/
%let mip1 = .0050; /*Monthly FHA MIP for LTV > 95 and term < 15*/
%let mip2 = .0025; /*Monthly FHA MIP for LTV < 95  and term < 15*/
%let mip3 = .0115; /*Monthly FHA MIP for LTV > 95  and term > 15*/
%let mip4 = .0110; /*Monthly FHA MIP for LTV < 95  and term > 15*/
%let freq = 11.81135; /*Frequency for SAS IRR function*/
%let l_limit = 417000; /*Default loan limit if not found in Eclipse*/
/*FOR SCORING EACH TRANSATION*/

%let pi_score = .02020202; /*transaction score for monthly payment savings - scored as for each $ saved*/
%let co_score = .000105005; /*transaction score for cashout available - scored as for each $ saved*/
%let lol_score = .000103567; /*transaction score for life of loan interest savings  - scored as for each $ saved*/
%let tr_score = .01666889; /*transaction score for term reduced of exisiting loan - scored as for every year reduced*/
%let arm_score = 0; /*Discount factor for an ARM offer*/

/*CREATES A TEMPORARY FILE FOR ALL TRANSACTION TYPES AND POSSIBLE TERM REDUCTIONS BASED ON THE 
NEAREST ADJUSTED REMAINING TERM OF THE LOAN ROUNDED OF TO 60

A SAMPLE OF THE DATA CARDS IS PLACED IN THE SHARED DRIVE PATH BELOW - 
PLEASE USE THE EXCEL TO MANIPULATE TRANSACTIONS IN FUTURE
PATH

*/
data tran;                      
input TRANSACTION $ 1-19	REM_TERM	NEW_TERM; 
cards;           
RATE TERM REFINANCE 360 360
RATE TERM REFINANCE 300 360
RATE TERM REFINANCE 240 240
RATE TERM REFINANCE 180 180
RATE TERM REFINANCE 120 120
CASHOUT SAME PI     360 360
CASHOUT SAME PI     300 360
CASHOUT SAME PI     240 240
CASHOUT SAME PI     180 180
CASHOUT SAME PI     120 120
CASHOUT 80 LTV      360 360
CASHOUT 80 LTV      300 360
CASHOUT 80 LTV      240 240
CASHOUT 80 LTV      180 180
CASHOUT 80 LTV      120 120
TERM EXTENDER       300 360
TERM EXTENDER       240 360
TERM EXTENDER       180 240
TERM EXTENDER       180 360
TERM EXTENDER       120 180
TERM EXTENDER       120 240
TERM EXTENDER       120 360
TERM REDUCER        360 240
TERM REDUCER        360 180
TERM REDUCER        360 120
TERM REDUCER        300 240
TERM REDUCER        300 180
TERM REDUCER        300 120
TERM REDUCER        240 180
TERM REDUCER        240 120
TERM REDUCER        180 120
;
run; 

/*CREATES A TEMPORARY TABLE OF POSSIBLE LOAN TYPES FOR A PRODUCT GROUP
PRODUCT GROUP DEFINED AS LTV 
UNDER 80 = C
80 - 97.5 AND INVESTOR NOT FANNIE OR FREDDIE = F
97.5 AND OVER AND INVESTOR FANNIE OR FREDDIE = H
UNPAID BALANCE OVER 417K = J*/

data prod;
input PROD_GROUP $char1. TYPE $char4.;
cards;
CC
CARM5
CARM7
FF
HH
JJF
JJA
BHBF
BHBA
;
run;

/*PULL LOANS FROM ECR AND CREATE A TEMPORARY TABLE*/
PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=ECR_RPT ORAPW='PRODRPT22' PATH="GMEDAPP") ;
                                    
                                    CREATE table LOANS1 AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT 
A.ACCOUNT_NUMBER,
A.UNPAID_BALANCE,
B.CURRENT_PROPERTY_VALUE,
A.PRINCIPAL_AND_INTEREST,
A.INTEREST_RATE,
A.INVESTOR_FULL_NAME,
A.ORIGINATION_DATE,
C.STATE,
C.ZIP,
A.RATE_TYPE
FROM ECR_CORE.LOAN A, ECR_CORE.ADDRESS C, ECR_CORE.PROPERTY B
WHERE (A.PROPERTY_ID = B.PROPERTY_ID) 
AND (B.ADDRESS_ID = C.ADDRESS_ID) 
AND GMAC_OWNED_FLAG = 'Y' 
AND LIEN_POSITION ='01' 
AND (LOAN_STATUS_CODE = '1' OR LOAN_STATUS_CODE = '9')
);
DISCONNECT FROM CONN1;
QUIT;

/*GET JUMBO LOAN LIMITS BY ZIP CODE FROM ECLIPSE*/
PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_ADHOC ORAPW='MRDM$101' PATH="DTRPTP") ;
                                    
                                    CREATE table loan_limit AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(select ZIP_CODE, CONFORMING_ONE_UNIT_LOAN_LIMIT AS LOAN_LIMIT 
from PUB.ZIP_MSA_XREF_MV
);
DISCONNECT FROM CONN1;
QUIT;


/*GET LATEST RATESHEET FROM ECLIPSE*/

PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_ADHOC ORAPW='MRDM$101' PATH="DTRPTP") ;
                                    
                                    CREATE table RATESHEET AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT PRODUCT, RATE_DT, POINTS, MIN(RATE) AS RATE
FROM
(
SELECT
CASE WHEN RH2.PROD_CD = '30' AND TIER_CD = 'CONF' THEN '30C'
WHEN RH2.PROD_CD = '15' AND TIER_CD = 'CONF' THEN '15C'
WHEN RH2.PROD_CD = '30FH' THEN 'FHA'
WHEN RH2.PROD_CD = '925' AND TIER_CD = 'JUMB' THEN 'JUMBO ARM'
WHEN RH2.PROD_CD = '30' AND TIER_CD = 'JUMB' THEN 'JUMBO FIXED'
WHEN RH2.PROD_CD = 'V68' THEN '30HASP'
WHEN RH2.PROD_CD = '5/1C' THEN 'ARM5'
WHEN RH2.PROD_CD = 'B14' THEN 'ARM7'
WHEN RH2.PROD_CD = 'V41' THEN 'HB 30'
WHEN RH2.PROD_CD = 'V40' THEN 'HB 15'
WHEN RH2.PROD_CD = 'X48' THEN 'HB 20'
WHEN RH2.PROD_CD = 'X38' THEN 'HB 10'
WHEN RH2.PROD_CD = 'V42' THEN 'HB ARM'
ELSE 'UNKNOWN' END AS PRODUCT,
RH2.RATE_DT as rate_dt,
RH2.RATE,
CASE WHEN RH2.PROD_CD='5/1C' OR PROD_CD = 'B14' then RH2.PTS_045 else RH2.PTS_005 end as POINTS
FROM PUB.PT_T_RATEHIST RH2
WHERE  	RH2.RATE_NO = (SELECT  MAX(RAT2.RATE_NO) FROM  PUB.PT_T_RATEHIST  RAT2 WHERE RAT2.RATE_NO < 1000000)
and RH2.ENTITY_CD='GMAC' AND RH2.CHAN_CD='RETL'
AND RH2.SCHAN_CD IN ('NT', 'NTHCTL')
AND ( (RH2.PROD_CD='30' and RH2.TIER_CD='CONF') OR (RH2.PROD_CD='15' and RH2.TIER_CD='CONF')
OR RH2.PROD_CD in ('30FH','V68')
OR RH2.PROD_CD in ('V41', 'V40','X48', 'X38', 'V42')
OR (RH2.PROD_CD='5/1C' and RH2.TIER_CD='CONF')
OR (RH2.PROD_CD='30' and RH2.TIER_CD='JUMB')
OR (RH2.PROD_CD='925' and RH2.TIER_CD='JUMB')
OR (RH2.PROD_CD='B14' and RH2.TIER_CD='CONF'))
ORDER BY
CASE WHEN RH2.PROD_CD = '30' AND TIER_CD = 'CONF' THEN '30C'
WHEN RH2.PROD_CD = '15' AND TIER_CD = 'CONF' THEN '15C'
WHEN RH2.PROD_CD = '30FH' THEN 'FHA'
WHEN RH2.PROD_CD = '925' AND TIER_CD = 'JUMB' THEN 'JUMBO ARM'
WHEN RH2.PROD_CD = '30' AND TIER_CD = 'JUMB' THEN 'JUMBO FIXED'
WHEN RH2.PROD_CD = 'V68' THEN '30HASP'
WHEN RH2.PROD_CD = '5/1C' THEN 'ARM5'
WHEN RH2.PROD_CD = 'B14' THEN 'ARM7'
WHEN RH2.PROD_CD = 'V41' THEN 'HB 30'
WHEN RH2.PROD_CD = 'V40' THEN 'HB 15'
WHEN RH2.PROD_CD = 'X48' THEN 'HB 20'
WHEN RH2.PROD_CD = 'X38' THEN 'HB 10'
WHEN RH2.PROD_CD = 'V42' THEN 'HB ARM'
ELSE 'UNKNOWN' END,
RH2.RATE)
GROUP BY PRODUCT, RATE_DT, POINTS
ORDER BY PRODUCT
);
DISCONNECT FROM CONN1;
QUIT;

/*GET LIBOR, MARGIN AND LIFETIME CAP FOR ARMS FROM ECLIPSE*/
PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_ADHOC ORAPW='MRDM$101' PATH="DTRPTP") ;
                                    
                                    CREATE table MARGIN AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
select a.rate_dt, a.type, a.margin, a.indx, a.life_cap,
b.FIRST_ADJ, b.FIRST_INC, b.SEC_ADJ, b.SEC_INC
from
(
select distinct
rate_dt,
case when prod_cd = '5/1C' then 'ARM5'
when prod_cd = 'B14' then 'ARM7'
else 'JA' end as type,
margin,
indx,
life_cap
from PUB.RATEINDX@DTRPTP RH2
WHERE RH2.RATE_NO = (SELECT  MAX(RAT2.RATE_NO) FROM  PUB.PT_T_RATEHIST@DTRPTP RAT2 WHERE RAT2.RATE_NO < 1000000)
and (prod_cd = '5/1C' or prod_cd = 'B14' or prod_cd = '925')
) a,
(
select
case when prod_cd = '5/1C' then 'ARM5'
when prod_cd = '71L' then 'ARM7'
else 'JA' end as type,
FIRST_ADJ, FIRST_INC, SEC_ADJ, SEC_INC, LIFE_CAP
from PUB.PRODUCT@DTRPTP where
ENTITY_CD='GMAC' AND CHAN_CD='RETL'
and
prod_cd in ('5/1C', '5/1','71L')
) b
where a.type = b.type
);
DISCONNECT FROM CONN1;
QUIT;

PROC SQL;
CREATE TABLE M1 AS SELECT rate_dt, 'HBA' AS type, margin, indx, life_cap,
FIRST_ADJ, FIRST_INC, SEC_ADJ, SEC_INC FROM MARGIN WHERE TYPE = 'ARM5';
QUIT;

PROC APPEND BASE = MARGIN DATA = M1;
RUN;

/*INITIALIZE CSRG FOR PRICING RULES*/

libname RULES oracle user=CSRG_PROD password='mrdm$101' path=RENPU schema=CSRG_PROD;
/*THE FOLLOWING SET OF STATEMENTS AND MACROS PULL THE MOST RECENT RATES 
FOR C30, C20, C15, C10, ARM 5/1, HASP, FHA, JUMBO ARM & CASHOUT 80 LTV

PLEASE NOTE THAT 

1.FOR ANY NEW PRODUCT ADDITIONS SIGNIFICANT CHANGES TO THE EXCEL WROKBOOK AND CODE WILL HAVE TO BE MADE
2. THOUGH JUMBO ARM RATES ARE PULLED CURRENT MARKETING DOES NOT MAKE JUMBO SOLICITATIONS 
   AS ARM APR CALCULATIONS ARE COMPLEX */
proc sql noprint;
select count(*) 
into : numrows
from (select distinct rule_code from RULES.PRICING);
%let numrows = &numrows;
select rule_code
into : rule_code1- : rule_code&numrows
from (select distinct rule_code from RULES.PRICING);

select count(*) 
into : prodrows
from (select distinct prod from RULES.PRICING where prod ne 'ALL');
%let prodrows = &prodrows;
select prod
into : prod1- : prod&numrows
from (select distinct prod from RULES.PRICING  where prod ne 'ALL');

select distinct rule_code into : conv separated by ' + ' from RULES.PRICING
where (prod = 'C30' or prod = 'ALL');

select distinct rule_code into : gov separated by ' + ' from RULES.PRICING
where (prod = 'FHA' or prod = 'ALL');

select distinct rule_code into : jum separated by ' + ' from RULES.PRICING
where (prod = 'JUM' or prod = 'ALL');

select distinct rule_code into : has separated by ' + ' from RULES.PRICING
where (prod = 'HAS' or prod = 'ALL');

select distinct rule_code into : c20 separated by ' + ' from RULES.PRICING
where (prod = 'C20' or prod = 'C30' or prod = 'ALL');

select distinct rule_code into : coo separated by ' + ' from RULES.PRICING
where (prod = 'COO');

select distinct rule_code into : c15 separated by ' + ' from RULES.PRICING
where (prod = 'C15' or prod = 'C30' or prod = 'ALL');

select distinct rule_code into : c10 separated by ' + ' from RULES.PRICING
where (prod = 'C10' or prod = 'C30' or prod = 'ALL');

select distinct rule_code into : arm5 separated by ' + ' from RULES.PRICING
where (prod = 'ARM5' or prod = 'C30' or prod = 'ALL');

select distinct rule_code into : arm7 separated by ' + ' from RULES.PRICING
where (prod = 'ARM7' or prod = 'C30' or prod = 'ALL');

select distinct rule_code into : a5c separated by ' + ' from RULES.PRICING
where (prod = 'A5C' or prod = 'COO');

select distinct rule_code into : a7c separated by ' + ' from RULES.PRICING
where (prod = 'A7C' or prod = 'COO');

select distinct rule_code into : ctc separated by ' + ' from RULES.PRICING
where (prod = 'CTC' or prod = 'COO');

select distinct rule_code into : cfc separated by ' + ' from RULES.PRICING
where (prod = 'CFC' or prod = 'COO');

select distinct rule_code into : c1c separated by ' + ' from RULES.PRICING
where (prod = 'C1C' or prod = 'COO');

select distinct rule_code into : jum1 separated by ' + ' from RULES.PRICING
where (prod = 'JUM1' or prod = 'ALL');

select distinct rule_code into : hb30 separated by ' + ' from RULES.PRICING
where (prod = 'C30' or prod = 'ALL' or prod = 'HB30');

select distinct rule_code into : hb20 separated by ' + ' from RULES.PRICING
where (prod = 'C30' or prod = 'ALL' or prod = 'HB20');

select distinct rule_code into : hb15 separated by ' + ' from RULES.PRICING
where (prod = 'C30' or prod = 'ALL' or prod = 'HB15');

select distinct rule_code into : hb10 separated by ' + ' from RULES.PRICING
where (prod = 'C30' or prod = 'ALL' or prod = 'HB10');

select distinct rule_code into : hba separated by ' + ' from RULES.PRICING
where (prod = 'C30' or prod = 'ALL' or prod = 'HBA');

quit;
 /*Creates a temporary table from ECR.LOAN based on conditions specified in 'Loan_Selection' Tab of the workbook
   Conditions relating to tables other than ECR.LOAN need to be specified with adequate joins
   For instance below CURRENT_PROPERTY_VALUE from table ECR.PROPERTY has been included*/

PROC SQL;
CREATE TABLE LOANS AS SELECT
ACCOUNT_NUMBER,
ROUND(UNPAID_BALANCE,1) AS UNPAID_BALANCE,
ROUND(CURRENT_PROPERTY_VALUE,1) AS CURRENT_PROPERTY_VALUE,
ROUND(PRINCIPAL_AND_INTEREST,1) AS PRINCIPAL_AND_INTEREST,
CASE WHEN CURRENT_PROPERTY_VALUE > 0 THEN ROUND(UNPAID_BALANCE/CURRENT_PROPERTY_VALUE,.001) 
		  ELSE 0 END AS LTV,
ROUND(MORT(UNPAID_BALANCE, PRINCIPAL_AND_INTEREST, INTEREST_RATE/12, .),1) AS REMAINING_TERM,
INTEREST_RATE,
INVESTOR_FULL_NAME,
ORIGINATION_DATE,
STATE,
RATE_TYPE,
CASE WHEN B.LOAN_LIMIT = . THEN &l_limit ELSE B.LOAN_LIMIT END AS LOAN_LIMIT
FROM LOANS1 AS A LEFT JOIN LOAN_LIMIT AS B ON A.ZIP = B.ZIP_CODE
WHERE CURRENT_PROPERTY_VALUE > 0 /*for missing AVMs*/
AND (UNPAID_BALANCE * INTEREST_RATE/12)/PRINCIPAL_AND_INTEREST < 0.95 /*remove IO loans*/;

CREATE TABLE TEMP AS SELECT 
ACCOUNT_NUMBER,
UNPAID_BALANCE AS AMT,
LTV,
CURRENT_PROPERTY_VALUE AS PROP_VAL,
STATE
FROM LOANS;

/*DROP TABLE LOANS1;*/

QUIT;

/*Following macros pull pricing conditions from the Wrokbook and then apply them to loan properties to
  pull the most recent rates from Eclipse for all loans*/

%macro pricing_conditions;
%local i;
%do i = 1 %to &numrows;
%put "&&rule_code&i";
%GLOBAL &&rule_code&i;
proc sql noprint;
select condition into : &&rule_code&i separated by ' ' 
from RULES.PRICING 
where rule_code = "&&rule_code&i";
quit;
%end;
%mend pricing_conditions;
%pricing_conditions

%macro add_llpa;
%local i;
%do i = 1 %to &numrows;
%let a = &&rule_code&i;
%let t = %str(&&rule_code&i);
%if &i = 1 %then %do;
%let fn = temp;
%end;
%else %do;
%let z = %eval(&i-1);
%let fn = %str(temp&z);
%end;
%put &fn;
proc sql;
create table temp&i as select *, 
&&&a as &&t
from &fn;
quit;
%end
;
%mend add_llpa;
%add_llpa

PROC SQL;
CREATE TABLE LLPA AS SELECT 
ACCOUNT_NUMBER,
&conv AS ADJC30,
&gov AS ADJFHA,
&has AS ADJHAS,
&jum AS ADJJUM,
&c20 AS ADJC20,
&coo AS ADJCOO,
&c15 AS ADJC15,
&c10 AS ADJC10,
&arm5 AS ADJARM5,
&arm7 AS ADJARM7,
&a5c AS ADJA5C,
&a7c AS ADJA7C,
&ctc AS ADJCTC,
&cfc AS ADJCFC,
&c1c AS ADJC1C,
&jum1 as ADJJUM1,
&hb30 as ADJHB30,
&hb20 as ADJHB20,
&hb15 as ADJHB15,
&hb10 as ADJHB10,
&hba as ADJHBA
FROM TEMP&numrows;
QUIT;

%macro del_tempfiles1;
%local i;
%let c = %eval(&numrows+1);
%do i = 1 %to &c;
%if &i = 1 %then %do;
%let fn = temp;
%end;
%else %do;
%let z = %eval(&i-1);
%let fn = %str(temp&z);
%end;
proc sql;
drop table &fn;
quit;
%end;
%mend del_tempfiles1;
%del_tempfiles1;

%macro pull_rate;
%local i;
%do i = 1 %to &prodrows;
%let a = &&prod&i;
%let t = %str(ADJ&&prod&i);
%let r = %str(RATE&&prod&i);
%let p = %str(NETPOINTS&&prod&i);
%if &i = 1 %then %do;
%let fn = llpa;
%end;
%else %do;
%let z = %eval(&i-1);
%let fn = %str(llpa&z);
%end;
proc sql;
create table llpa&i as
select a.*, 
b.&&p,
b.&&r
from &fn as a
left join
(
select 
B1.ACCOUNT_NUMBER,
B1.&&t,
(r2.points + b1.&&t)as &&p,
r2.rate as &&r
from llpa as b1, RATESHEET as r2
where (((%str("&&prod&i") in ('JUM', 'ARM5', 'ARM7', 'A5C', 'A7C', 'HBA')) AND 
((r2.points + b1.&&t) >= 0)) OR
((%str("&&prod&i") in ('C30', 'COO', 'FHA', 'HAS', 'C20', 'C15', 'C10', 'CTC', 'CFC', 'C1C', 'JUM1', 'HB30', 'HB20', 'HB15', 'HB10')) AND 
((r2.points + b1.&&t) >= 1)))
and 
r2.product = (
case when %str("&&prod&i") = 'C30' then '30C'
when %str("&&prod&i") = 'COO' then  '30C'
when %str("&&prod&i") = 'FHA' then  'FHA'
when %str("&&prod&i") = 'HAS' then  '30HASP'
when %str("&&prod&i") = 'C20' then  '30C'
when %str("&&prod&i") = 'JUM' then  'JUMBO ARM' 
when %str("&&prod&i") = 'C15' then  '15C'
when %str("&&prod&i") = 'C10' then  '15C' 
when %str("&&prod&i") = 'ARM5' then  'ARM5'
when %str("&&prod&i") = 'ARM7' then  'ARM7'
when %str("&&prod&i") = 'A5C' then  'ARM5'
when %str("&&prod&i") = 'A7C' then  'ARM7'
when %str("&&prod&i") = 'CTC' then  '30C'
when %str("&&prod&i") = 'CFC' then  '15C'
when %str("&&prod&i") = 'C1C' then  '15C'
when %str("&&prod&i") = 'JUM1' then  'JUMBO FIXED'
when %str("&&prod&i") = 'HB30' then  'HB 30'
when %str("&&prod&i") = 'HB20' then  'HB 20'
when %str("&&prod&i") = 'HB15' then  'HB 15'
when %str("&&prod&i") = 'HB10' then  'HB 10'
when %str("&&prod&i") = 'HBA' then  'HB ARM'
end)
group by b1.account_number, b1.&&t
having ((r2.points + b1.&&t) = min((r2.points + b1.&&t)))
) as b
on( a.account_number = b.account_number);
quit;
%end;
%mend pull_rate;
%pull_rate

PROC SQL;
CREATE TABLE RATES AS SELECT 
*
FROM llpa&prodrows;
QUIT;

%macro del_tempfiles2;
%local i;
%let c = %eval(&prodrows+1);
%do i = 1 %to &c;
%if &i = 1 %then %do;
%let fn = llpa;
%end;
%else %do;
%let z = %eval(&i-1);
%let fn = %str(llpa&z);
%end;
proc sql;
drop table &fn;
quit;
%end;
%mend del_tempfiles2;
%del_tempfiles2;

libname RULES clear;


proc sql;
create table nl1 as select 
account_number, 
unpaid_balance, 
current_property_value, 
principal_and_interest,
remaining_term,
loan_limit,
CASE WHEN roundz(remaining_term,60) < 120 THEN 120
WHEN roundz(remaining_term,60) > 280 THEN 360
ELSE roundz(remaining_term,60) END as ad_term, 
case when ltv < .80000001 then 
case when state in ('HI', 'AK') and (unpaid_balance >= 625500 and unpaid_balance < loan_limit) then 'B'
when state not in ('HI', 'AK') and (unpaid_balance >= 417000 and unpaid_balance < loan_limit) then 'B'
when unpaid_balance >= loan_limit then 'J' 
else 'C' end
when ltv >= .8000001 and ltv < .975 and unpaid_balance <= 417000 then case when  
(INVESTOR_FULL_NAME = 'FANNIE MAE' OR INVESTOR_FULL_NAME = 'FREDDIE MAC') 
          AND  ORIGINATION_DATE < '28Feb2009:0:0:0'dt then 'H' else 'F' end
when ltv >= .975 and ltv < 1.25 and unpaid_balance <= 417000 then case when  
(INVESTOR_FULL_NAME = 'FANNIE MAE' OR INVESTOR_FULL_NAME = 'FREDDIE MAC') 
          AND  ORIGINATION_DATE < '28Feb2009:0:0:0'dt then 'H' else 'O' end
else 'O' end as prod_group
from loans;
quit;

proc sql;
create table nl2 as select 
a.*, 
b.type
from nl1 as a, prod as b
where a.prod_group = b.prod_group;

create table nl3 as select distinct
a.*, 
b.transaction, 
case when a.type in ('ARM5', 'ARM7', 'HBA', 'JA') then 360 else b.new_term end as new_term
from nl2 as a, tran as b 
where a.ad_term = b.rem_term;
quit;

/*OMISSION CONDITIONS BASED ON TRANSATION TYPES FOR INSTANCE
THERE CAN BE NO TERM REDUCTION OR CASHOUT FOR FHA PRODUCTS 
OR ALTERNATIVELY 
CASHOUT CANNOT BE OFFERED IF 80% OF PORPERTY VALUE IS GREATER THAN 417K
						OR IF 80% OF PROPERTY VALUE - 10K IS LESS THAN UPB

ONE CAN REVIEW AND DELETE THESE STEPS IF NEED BE*/

proc sql;
delete from nl3 
where 
(transaction = 'TERM EXTENDER' and prod_group = 'H') /*remove term extension offers for HASP*/
or (transaction = 'CASHOUT 80 LTV' AND PROD_GROUP = 'H') /*remove cashout offers for HASP*/
OR (transaction = 'CASHOUT SAME PI' AND PROD_GROUP = 'H') /*remove cashout offers for HASP*/
OR (transaction = 'TERM EXTENDER' and prod_group = 'F') /*remove term extension offers for FHA*/
or (transaction = 'CASHOUT 80 LTV' AND PROD_GROUP = 'F') /*remove term extension offers for FHA*/
OR (transaction = 'CASHOUT SAME PI' AND PROD_GROUP = 'F') /*remove term extension offers for FHA*/
OR (transaction = 'CASHOUT SAME PI' AND TYPE IN ('ARM5', 'ARM7', 'JA', 'HBA')) /*remove cashout same pi offers for ARM*/
OR (transaction = 'TERM REDUCER' AND TYPE IN ('ARM5', 'ARM7', 'JA', 'HBA')) /*remove term reducer offers for ARM as term is constant 360*/
OR (transaction = 'RATE TERM REFINANCE' AND TYPE IN ('ARM5', 'ARM7', 'JA', 'HBA') and ad_term < 300) /* remove all ARM offers from RATE TERM where adjusted remaining term is less than 300*/
/*OR (transaction = 'TERM EXTENDER' AND TYPE IN ('ARM5', 'ARM7', 'J')) remove term extension offers for ARM as term is constant 360*/
OR (transaction = 'CASHOUT 80 LTV' 
AND (CURRENT_PROPERTY_VALUE * .8) <= UNPAID_BALANCE) 
/*remove loans with no cashout benefit from cashout transaction*/
OR (transaction = 'CASHOUT SAME PI' 
AND (CURRENT_PROPERTY_VALUE * .8) <= UNPAID_BALANCE)
/*remove loans with no cashout benefit from cashout transaction*/;
QUIT;

PROC SQL;
CREATE TABLE BPO1 AS SELECT DISTINCT
A.ACCOUNT_NUMBER,
A.TRANSACTION,
A.UNPAID_BALANCE,
A.CURRENT_PROPERTY_VALUE,
A.PRINCIPAL_AND_INTEREST,
A.REMAINING_TERM,
A.NEW_TERM AS NEW_TERM,
A.TYPE,
A.LOAN_LIMIT,
CASE WHEN A.TYPE = 'C' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR FIXED CONV'
WHEN A.TYPE = 'ARM5' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR ARM 5/1'
WHEN A.TYPE = 'ARM7' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR ARM 7/1'
WHEN A.TYPE = 'F' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR FIXED FHA'
WHEN A.TYPE = 'H' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR FIXED HASP'
WHEN A.TYPE = 'JF' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR JUMBO FIXED'
WHEN A.TYPE = 'JA' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR JUMBO ARM 5/1'
WHEN A.TYPE IN ('HBF') THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR HI BAL FIXED'
WHEN A.TYPE = 'HBA' THEN TRIM(PUT(NEW_TERM/12,2.)) || ' YR HI BAL ARM 5/1'
END AS NEW_PRODUCT,
CASE WHEN SUBSTR(A.TRANSACTION,1,7) = 'CASHOUT' THEN 
CASE WHEN A.TYPE = 'C' THEN 
		CASE WHEN A.NEW_TERM = 120 THEN B.RATEC1C
		WHEN A.NEW_TERM = 180 THEN B.RATECFC
		WHEN A.NEW_TERM = 240 THEN B.RATECTC
		WHEN A.NEW_TERM = 360 THEN B.RATECOO
		END
	WHEN A.TYPE = 'ARM5' THEN B.RATEA5C
	WHEN A.TYPE = 'ARM7' THEN B.RATEA7C
	WHEN A.TYPE = 'H' THEN B.RATEHAS
	WHEN A.TYPE = 'F' THEN B.RATEFHA
	WHEN A.TYPE = 'JA' THEN B.RATEJUM
	WHEN A.TYPE = 'JF' THEN B.RATEJUM1
	WHEN A.TYPE = 'HBA' THEN B.RATEHBA
	WHEN A.TYPE IN ('HBF') THEN
		CASE WHEN A.NEW_TERM = 120 THEN B.RATEHB10
		WHEN A.NEW_TERM = 180 THEN B.RATEHB15
		WHEN A.NEW_TERM = 240 THEN B.RATEHB20
		WHEN A.NEW_TERM = 360 THEN B.RATEHB30
		END
	END
ELSE
CASE WHEN A.TYPE = 'C' THEN 
		CASE WHEN A.NEW_TERM = 120 THEN B.RATEC10
		WHEN A.NEW_TERM = 180 THEN B.RATEC15
		WHEN A.NEW_TERM = 240 THEN B.RATEC20
		WHEN A.NEW_TERM = 360 THEN B.RATEC30
		END
	WHEN A.TYPE = 'ARM5' THEN B.RATEARM5
	WHEN A.TYPE = 'ARM7' THEN B.RATEARM7
	WHEN A.TYPE = 'H' THEN B.RATEHAS
	WHEN A.TYPE = 'F' THEN B.RATEFHA
	WHEN A.TYPE = 'JA' THEN B.RATEJUM
	WHEN A.TYPE = 'JF' THEN B.RATEJUM1
	WHEN A.TYPE = 'HBA' THEN B.RATEHBA
	WHEN A.TYPE IN ('HBF') THEN
		CASE WHEN A.NEW_TERM = 120 THEN B.RATEHB10
		WHEN A.NEW_TERM = 180 THEN B.RATEHB15
		WHEN A.NEW_TERM = 240 THEN B.RATEHB20
		WHEN A.NEW_TERM = 360 THEN B.RATEHB30
		END
	END
END AS NEW_RATE,

CASE WHEN SUBSTR(A.TRANSACTION,1,7) = 'CASHOUT' THEN 
CASE WHEN A.TYPE = 'C' THEN 
		CASE WHEN A.NEW_TERM = 120 THEN B.NETPOINTSC1C
		WHEN A.NEW_TERM = 180 THEN B.NETPOINTSCFC
		WHEN A.NEW_TERM = 240 THEN B.NETPOINTSCTC
		WHEN A.NEW_TERM = 360 THEN B.NETPOINTSCOO
		END
	WHEN A.TYPE = 'ARM5' THEN B.NETPOINTSA5C
	WHEN A.TYPE = 'ARM7' THEN B.NETPOINTSA7C
	WHEN A.TYPE = 'H' THEN B.NETPOINTSHAS
	WHEN A.TYPE = 'F' THEN B.NETPOINTSFHA
	WHEN A.TYPE = 'JA' THEN B.NETPOINTSJUM
	WHEN A.TYPE = 'JF' THEN B.NETPOINTSJUM1
	WHEN A.TYPE = 'HBA' THEN B.NETPOINTSHBA
	WHEN A.TYPE IN ('HBF') THEN
		CASE WHEN A.NEW_TERM = 120 THEN B.NETPOINTSHB10
		WHEN A.NEW_TERM = 180 THEN B.NETPOINTSHB15
		WHEN A.NEW_TERM = 240 THEN B.NETPOINTSHB20
		WHEN A.NEW_TERM = 360 THEN B.NETPOINTSHB30
		END
	END
ELSE
CASE WHEN A.TYPE = 'C' THEN 
		CASE WHEN A.NEW_TERM = 120 THEN B.NETPOINTSC10
		WHEN A.NEW_TERM = 180 THEN B.NETPOINTSC15
		WHEN A.NEW_TERM = 240 THEN B.NETPOINTSC20
		WHEN A.NEW_TERM = 360 THEN B.NETPOINTSC30
		END
	WHEN A.TYPE = 'ARM5' THEN B.NETPOINTSARM5
	WHEN A.TYPE = 'ARM7' THEN B.NETPOINTSARM7
	WHEN A.TYPE = 'H' THEN B.NETPOINTSHAS
	WHEN A.TYPE = 'F' THEN B.NETPOINTSFHA
	WHEN A.TYPE = 'JA' THEN B.NETPOINTSJUM
	WHEN A.TYPE = 'JF' THEN B.NETPOINTSJUM1
	WHEN A.TYPE = 'HBA' THEN B.NETPOINTSHBA
	WHEN A.TYPE IN ('HBF') THEN
		CASE WHEN A.NEW_TERM = 120 THEN B.NETPOINTSHB10
		WHEN A.NEW_TERM = 180 THEN B.NETPOINTSHB15
		WHEN A.NEW_TERM = 240 THEN B.NETPOINTSHB20
		WHEN A.NEW_TERM = 360 THEN B.NETPOINTSHB30
		END
	END
END AS NEW_NETPOINTS

FROM NL3 AS A, RATES AS B
WHERE A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER;
QUIT;

PROC SQL;
DROP TABLE NL1;
DROP TABLE NL2;
DROP TABLE NL3;
QUIT;


PROC SQL;
CREATE TABLE BPO2 AS SELECT
ACCOUNT_NUMBER,
TRANSACTION,
NEW_PRODUCT,
NEW_TERM,
NEW_LOAN_AMOUNT,
NEW_RATE,
TYPE,
FIRST_ADJ,
ARM_UNPAID_BALANCE,

ARM5_RATE,
ARM7_RATE,
JUM_RATE,

CASE WHEN TYPE = 'ARM5' THEN
ROUND(MORT(ARM_UNPAID_BALANCE, ., ARM5_RATE/1200,NEW_TERM - FIRST_ADJ),1) 
WHEN TYPE = 'ARM7' THEN
ROUND(MORT(ARM_UNPAID_BALANCE, ., ARM7_RATE/1200,NEW_TERM - FIRST_ADJ),1)
WHEN TYPE = 'JA' THEN
ROUND(MORT(ARM_UNPAID_BALANCE, ., JUM_RATE/1200,NEW_TERM - FIRST_ADJ),1)
WHEN TYPE = 'HBA' THEN
ROUND(MORT(ARM_UNPAID_BALANCE, ., HBA5_RATE/1200,NEW_TERM - FIRST_ADJ),1)
ELSE 0 END AS POST_PI,

NEW_LOAN_AMOUNT - NEW_FEES_DOL AS NET_PROCEEDS,

CASE WHEN TYPE = 'F' THEN 
CASE WHEN (NEW_LOAN_AMOUNT/CURRENT_PROPERTY_VALUE) > .78 AND T_78 > 60
THEN 
ROUND((MORT(((NEW_LOAN_AMOUNT*&ufmip) -((NEW_LOAN_AMOUNT*(&ufmip-1)) + &fees_fixed + (NEW_LOAN_AMOUNT * NEW_NETPOINTS /100) + (NEW_LOAN_AMOUNT*&ufmip* NEW_RATE/2400) + (T_78* MIP * T_BAL/12))), NEW_PI,., NEW_TERM))*1200,.01)
ELSE 
ROUND((MORT(((NEW_LOAN_AMOUNT*&ufmip) -((NEW_LOAN_AMOUNT*(&ufmip-1)) + &fees_fixed + (NEW_LOAN_AMOUNT * NEW_NETPOINTS/100) + (NEW_LOAN_AMOUNT*&ufmip*NEW_RATE/2400) + (60 * MIP * (((NEW_LOAN_AMOUNT*&ufmip) + T_5)/2)/12))), NEW_PI,.,NEW_TERM))*1200,.01)
END 
WHEN TYPE = 'JA' THEN
ROUND(((MORT((NEW_LOAN_AMOUNT - (&fees_jumbo + (NEW_LOAN_AMOUNT*NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT*NEW_RATE/2400))),(MORT(NEW_LOAN_AMOUNT,.,JUM_RATE/1200,NEW_TERM)),.,NEW_TERM))*1200),.01)
WHEN TYPE = 'ARM5' THEN
ROUND(((MORT((NEW_LOAN_AMOUNT - (&fees_arm + (NEW_LOAN_AMOUNT*NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT*NEW_RATE/2400))),(MORT(NEW_LOAN_AMOUNT,.,ARM5_RATE/1200,NEW_TERM)),.,NEW_TERM))*1200),.01)
WHEN TYPE = 'ARM7' THEN
ROUND(((MORT((NEW_LOAN_AMOUNT - (&fees_arm + (NEW_LOAN_AMOUNT*NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT*NEW_RATE/2400))),(MORT(NEW_LOAN_AMOUNT,.,ARM7_RATE/1200,NEW_TERM)),.,NEW_TERM))*1200),.01)
WHEN TYPE = 'JF' THEN
ROUND(((MORT((NEW_LOAN_AMOUNT - (&fees_jumbo + (NEW_LOAN_AMOUNT*NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT*NEW_RATE/2400))),NEW_PI,.,NEW_TERM))*1200),.01)
ELSE
ROUND(((MORT((NEW_LOAN_AMOUNT - (&fees_fixed + (NEW_LOAN_AMOUNT*NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT*NEW_RATE/2400))),NEW_PI,.,NEW_TERM))*1200),.01) 
END AS NEW_APR,

NEW_POINTS_DOL,
NEW_FEES_DOL,
ROUND(NEW_NETPOINTS,.001) AS NEW_POINTS_PCT,
NEW_PI,
PRINCIPAL_AND_INTEREST - NEW_PI AS NEW_PI_SAVINGS_MO,
NEW_LOAN_AMOUNT - UNPAID_BALANCE AS NEW_CASHOUT,

CASE WHEN TYPE = 'ARM5' THEN ROUND((REMAINING_TERM * PRINCIPAL_AND_INTEREST) - (NEW_TERM * MORT(NEW_LOAN_AMOUNT,.,ARM5_RATE/1200,NEW_TERM)),1)
WHEN TYPE = 'ARM7' THEN ROUND((REMAINING_TERM * PRINCIPAL_AND_INTEREST) - (NEW_TERM * MORT(NEW_LOAN_AMOUNT,.,ARM7_RATE/1200,NEW_TERM)),1)
WHEN TYPE = 'JA' THEN ROUND((REMAINING_TERM * PRINCIPAL_AND_INTEREST) - (NEW_TERM * MORT(NEW_LOAN_AMOUNT,.,JUM_RATE/1200,NEW_TERM)),1)
WHEN TYPE = 'HBA' THEN ROUND((REMAINING_TERM * PRINCIPAL_AND_INTEREST) - (NEW_TERM * MORT(NEW_LOAN_AMOUNT,.,HBA5_RATE/1200,NEW_TERM)),1)
ELSE ROUND((REMAINING_TERM * PRINCIPAL_AND_INTEREST) - (NEW_TERM * NEW_PI),1) 
END AS LOL_SAVINGS,

REMAINING_TERM AS ADJ_REMAINING_TERM,
CURRENT_PROPERTY_VALUE AS MOST_RECENT_PROPERTY_VALUE,
MARGIN AS ARM_MARGIN,
LIBOR AS ARM_LIBOR,
LIFETIME_CAP AS ARM_LIFE_CAP

FROM
(
SELECT
*,
ROUND(NEW_NETPOINTS/100 * NEW_LOAN_AMOUNT,1) AS NEW_POINTS_DOL,

CASE WHEN TYPE = 'F' 
THEN ROUND(MORT((NEW_LOAN_AMOUNT* &ufmip),.,NEW_RATE/1200,A.NEW_TERM),1)
ELSE ROUND(MORT(NEW_LOAN_AMOUNT,.,NEW_RATE/1200,A.NEW_TERM),1) END AS NEW_PI,

CASE WHEN TYPE = 'F' THEN ROUND(&fees_fixed + ((NEW_LOAN_AMOUNT * NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT *&ufmip * NEW_RATE/2400))) 
WHEN TYPE = 'JA' OR TYPE = 'JF' THEN ROUND((&fees_jumbo +((NEW_LOAN_AMOUNT * NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT * NEW_RATE/2400))),.01)
ELSE 
CASE WHEN TYPE = 'ARM5' OR TYPE = 'ARM7' OR TYPE = 'HBA' THEN ROUND((&fees_arm + ((NEW_LOAN_AMOUNT * NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT * NEW_RATE/2400))),.01)
ELSE ROUND((&fees_fixed + ((NEW_LOAN_AMOUNT * NEW_NETPOINTS/100) +(NEW_LOAN_AMOUNT * NEW_RATE/2400))),.01) END 
END AS NEW_FEES_DOL,

MIN((MARGIN + LIBOR), (NEW_RATE + FIRST_INC)) AS ARM7_RATE,
MIN((MARGIN + LIBOR), (NEW_RATE + FIRST_INC)) AS ARM5_RATE,
MIN((MARGIN + LIBOR), (NEW_RATE + FIRST_INC)) AS JUM_RATE,
MIN((MARGIN + LIBOR), (NEW_RATE + FIRST_INC)) AS HBA5_RATE,

NEW_LOAN_AMOUNT - ROUND(((MORT(NEW_LOAN_AMOUNT, ., NEW_RATE/1200, NEW_TERM)/(NEW_RATE/1200)) - NEW_LOAN_AMOUNT)*(((1+(NEW_RATE/1200))**FIRST_ADJ)-1),1) AS ARM_UNPAID_BALANCE,

MORT(NEW_LOAN_AMOUNT*&ufmip,.,NEW_RATE/1200,NEW_TERM) AS T_PI, /*FHA PI with UFMIP*/
NEW_TERM-(MORT(CURRENT_PROPERTY_VALUE*.78,(MORT(NEW_LOAN_AMOUNT*&ufmip,.,NEW_RATE/1200,NEW_TERM)),NEW_RATE/1200,.)) AS T_78, /*Time in months for ltv to reach 78% FHA)*/
((NEW_LOAN_AMOUNT * &ufmip) + (CURRENT_PROPERTY_VALUE*.78))/2 AS T_BAL, /*Avg. balance at 78% ltv*/
CASE WHEN TYPE = 'F' THEN 
CASE WHEN NEW_TERM <= 180 AND NEW_LOAN_AMOUNT/CURRENT_PROPERTY_VALUE >= .95 THEN &mip1
WHEN NEW_TERM <= 180 AND NEW_LOAN_AMOUNT/CURRENT_PROPERTY_VALUE < .95 THEN &mip1
WHEN NEW_TERM > 180 AND NEW_LOAN_AMOUNT/CURRENT_PROPERTY_VALUE >= .95 THEN &mip3
WHEN NEW_TERM > 180 AND NEW_LOAN_AMOUNT/CURRENT_PROPERTY_VALUE < .95 THEN &mip4
ELSE 0 END END AS MIP,
(NEW_LOAN_AMOUNT*&ufmip)*(1+NEW_RATE/1200)**60-(MORT(NEW_LOAN_AMOUNT*&ufmip,.,NEW_RATE/1200,NEW_TERM))*((1+NEW_RATE/1200)**60-1) / (NEW_RATE/1200) AS T_5 /*Unpaid balance after 5 yrs. FHA*/
FROM
(
SELECT 
A.*, 
CASE WHEN A.TRANSACTION = 'CASHOUT 80 LTV' THEN
CASE WHEN A.TYPE IN ('C','ARM5','ARM7') THEN CASE WHEN A.CURRENT_PROPERTY_VALUE * .8 >= 417000 THEN 417000
     ELSE ROUND(A.CURRENT_PROPERTY_VALUE * .8,1) END
	WHEN A.TYPE IN ('HBF', 'HBA') THEN CASE WHEN A.CURRENT_PROPERTY_VALUE * .8 >= LOAN_LIMIT THEN LOAN_LIMIT
     ELSE ROUND(A.CURRENT_PROPERTY_VALUE * .8,1) END
	ELSE ROUND(A.CURRENT_PROPERTY_VALUE * .8,1) END 
    WHEN A.TRANSACTION = 'CASHOUT SAME PI' THEN 
CASE WHEN (MORT(., A.PRINCIPAL_AND_INTEREST, A.NEW_RATE/1200, A.NEW_TERM)) < A.CURRENT_PROPERTY_VALUE * .8 THEN 
CASE WHEN A.TYPE IN ('C','ARM5','ARM7') THEN CASE WHEN (MORT(., A.PRINCIPAL_AND_INTEREST, A.NEW_RATE/1200, A.NEW_TERM))  >= 417000 THEN 417000
     ELSE ROUND(MORT(., A.PRINCIPAL_AND_INTEREST, A.NEW_RATE/1200, A.NEW_TERM),1) END
	WHEN A.TYPE IN ('HBF', 'HBA') THEN CASE WHEN (MORT(., A.PRINCIPAL_AND_INTEREST, A.NEW_RATE/1200, A.NEW_TERM))  >= LOAN_LIMIT THEN LOAN_LIMIT
     ELSE ROUND(MORT(., A.PRINCIPAL_AND_INTEREST, A.NEW_RATE/1200, A.NEW_TERM),1) END
	ELSE ROUND(MORT(., A.PRINCIPAL_AND_INTEREST, A.NEW_RATE/1200, A.NEW_TERM),1) END 
ELSE
CASE WHEN A.TYPE IN ('C','ARM5','ARM7') THEN CASE WHEN A.CURRENT_PROPERTY_VALUE * .8 >= 417000 THEN 417000
     ELSE ROUND(A.CURRENT_PROPERTY_VALUE * .8,1) END
	WHEN A.TYPE IN ('HBF', 'HBA') THEN CASE WHEN A.CURRENT_PROPERTY_VALUE * .8 >= LOAN_LIMIT THEN LOAN_LIMIT
     ELSE ROUND(A.CURRENT_PROPERTY_VALUE * .8,1) END
	ELSE ROUND(A.CURRENT_PROPERTY_VALUE * .8,1) END 
END
ELSE A.UNPAID_BALANCE END AS NEW_LOAN_AMOUNT,
B.MARGIN, 
B.INDX AS LIBOR, 
B.LIFE_CAP AS LIFETIME_CAP,
B.FIRST_ADJ,
B.FIRST_INC,
B.SEC_ADJ,
B.SEC_INC
FROM BPO1 AS A LEFT JOIN MARGIN AS B ON
A.TYPE = B.TYPE
)
)
;
QUIT;

data arm_apr; 
  set BPO2;

if new_term = 360 and first_adj = 36 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 360);
      %if &i. <= 36 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 360 and first_adj = 60 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 360);
      %if &i. <= 60 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 360 and first_adj = 84 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 360);
      %if &i. <= 84 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;


if new_term = 240 and first_adj = 36 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 240);
      %if &i. <= 36 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 240 and first_adj = 60 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 240);
      %if &i. <= 60 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 240 and first_adj = 84 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 240);
      %if &i. <= 84 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;


if new_term = 180 and first_adj = 36 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 180);
      %if &i. <= 36 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 180 and first_adj = 60 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 180);
      %if &i. <= 60 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 180 and first_adj = 84 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 180);
      %if &i. <= 84 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

if new_term = 120 and first_adj = 36 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 120);
      %if &i. <= 36 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 120 and first_adj = 60 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 120);
      %if &i. <= 60 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

else if new_term = 120 and first_adj = 84 then do;
%macro arm_apr;
   new_apr1 = IRR( &freq, -1*net_proceeds
   %let i = 1;
   %do %while (&i. <= 120);
      %if &i. <= 84 %then %do;
         , new_pi
      %end;
	  %else %do;
         , post_pi
      %end;
      %let i = %eval(&i. + 1);
   %end; 
       );
%mend arm_apr;
%arm_apr;
end;

run;


PROC SQL;
CREATE TABLE BPO3 AS SELECT
A.ACCOUNT_NUMBER,
A.TRANSACTION,
A.NEW_PRODUCT,
A.NEW_TERM,
A.NEW_LOAN_AMOUNT,
A.NEW_RATE,
CASE WHEN TYPE IN ('ARM5', 'ARM7', 'JA', 'HBA') 
THEN ROUND(A.NEW_APR1,.001) ELSE ROUND(A.NEW_APR,.001) END AS NEW_APR,
A.NEW_POINTS_DOL,
A.NEW_FEES_DOL,
A.NEW_POINTS_PCT,
A.NEW_PI,
A.NEW_PI_SAVINGS_MO,
A.NEW_CASHOUT,
A.ADJ_REMAINING_TERM - A.NEW_TERM AS TERM_REDUCTION,
A.LOL_SAVINGS,
A.ADJ_REMAINING_TERM,
A.MOST_RECENT_PROPERTY_VALUE,
A.ARM_MARGIN,
A.ARM_LIBOR,
A.ARM_LIFE_CAP,
B.RATE_DT AS RATE_DT,
CASE WHEN A.NEW_PRODUCT CONTAINS 'ARM' OR A.NEW_PRODUCT CONTAINS 'HI BAL' OR A.NEW_PRODUCT CONTAINS 'JUMBO'
OR C.MARKETABLE NE 'Marketable'
THEN 'N' ELSE 'Y' END AS CSA_ELIGIBILITY_FLAG,
ROUND((CASE WHEN A.NEW_PI_SAVINGS_MO = . THEN 0 ELSE A.NEW_PI_SAVINGS_MO END * &pi_score) 
+ (CASE WHEN A.NEW_CASHOUT = . THEN 0 ELSE A.NEW_CASHOUT END * &co_score) + 
(LOL_SAVINGS * &lol_score) + ((ADJ_REMAINING_TERM - NEW_TERM) * &tr_score),.00001) AS TRANSACTION_SCORE
FROM ARM_APR AS A, (SELECT DISTINCT RATE_DT FROM RATESHEET) AS B, EXCLUSIONS AS C
WHERE A.ACCOUNT_NUMBER = C.ACCOUNT_NUMBER
ORDER BY A.ACCOUNT_NUMBER;
QUIT;

PROC SQL;
DELETE FROM BPO3 WHERE (TRANSACTION = 'CASHOUT 80 LTV' OR TRANSACTION = 'CASHOUT SAME PI') AND NEW_CASHOUT < 0;
QUIT;
 
PROC RANK DATA=BPO3 OUT=BATCH_PRICING_OUTPUT TIES=LOW DESCENDING;
BY ACCOUNT_NUMBER;
VAR TRANSACTION_SCORE;
RANKS TRANSACTION_RANK;
RUN;

PROC SQL;
CREATE INDEX ACCOUNT_NUMBER ON BATCH_PRICING_OUTPUT(ACCOUNT_NUMBER);
QUIT;

libname ECR oracle user=ECR_SAS password='ecr#379sas' path=GMEDAPP schema=ECR_CORE oracle_73=no;

PROC SQL;
DELETE FROM ECR.USER_EVERREADY_RESULTS;
QUIT;

proc append base = ecr.user_everready_results data = work.batch_pricing_output;
run;

libname ECR clear;


