/*--------------------------------------MRG PORTFOLIO BATCH PRICING ------------------------------*/

/*--------------------------------------VERSION DATE - 06/29/2012 --------------------------------*/


/*
THIS CURRENT VERSION OF BATCH PRICING DOES THE FOLLOWING
1. PULLS ALL RELEVANT LLPA'S FROM ECLIPSE
2. PULLS ALL ECR LOANS - GMAC OWNED ACTIVE FIRST LIENS WITH AVAILABLE CURRENT PROPERTY VALUES
3. USES A TEMPORARY TABLE CSRG_PROD.BATCH_PRICING_PRODUCTS FOR PRODUCTS TO BE PRICED - ANY PRODUCT 
   MODIFICATIONS HAVE TO BE ENTERED IN THAT TABLE
4. APPLIES ALL LLPA'S FOR THE PRODUCT TO PORTFOLIO LOANS
5. PICKS UP A RATE CLOSEST TO 1 POINT RATE FOR ALL LOANS FROM ECLIPSE RATESHEET
6. CALCULATES STATISTICS LIKE - PI SAVINGS, LIFE OF LOAN SAVINGS, CASHOUT AVAILABLE FOR ALL TRANSACTIONS
7. SCORES TRANSACTIONS
8. FLAGS NON CSA ELIGIBLE LOANS 
9. SAVES THE OUTPUT TO CSRG_PROD.BATCH_PRICING_OUPUT AND ECR_CORE.USER_EVERREADY_RESULTS
*/

/*Review the macros variables assigned below for calculations*/

%let l_limit = 417000; /*Default loan limit for all state other than HI and AK*/
%let l_limit_h = 625500; /*Default loan limit for all states of HI and AK*/
%let cred_score = 721; /*Default FICO for pricing*/

/*Since the ratesheet treats Hi Balance products as JUMB Tier type -
delete from Hi Balance product group Jumbo Product combinations*/

%let hi_bal = DELETE FROM NL1 WHERE LOAN_AMT <= LOAN_LIMIT AND ENG_PROD_CD IN ('004', '925');

/*delete from Hi Balance product group Jumbo Product combinations*/

%let jumbo = DELETE FROM NL1 WHERE LOAN_AMT > LOAN_LIMIT AND PROG_TYP = 'HBAL';

/*For FHA only retain LTV > 97.75 for VA to VA streamline*/ 
%let va = DELETE FROM NL1 WHERE PRODUCT_TYPE_CODE NOT IN (3, 6, 9, 2, 5, 8) AND NEW_PRODUCT LIKE ('%FHA%') AND LTV >= 97.75;

/*As looping over 300 LLPA conditions across 11 Million transactions takes way too much computing
time the LLPA's have been grouped together in batches of 100; the macro below will need revisiting in
case the total count of LLPA's goes over 399 at any given point*/

%macro llpa;
proc sql noprint;

select distinct sqlstr1 into : rule_code1 separated by ' + ' from llpa
where rownum < 100;

select distinct sqlstr1 into : rule_code2 separated by ' + ' from llpa
where rownum >= 100 and rownum < 200;

select distinct sqlstr1 into : rule_code3 separated by ' + ' from llpa
where rownum >= 200 and rownum < 300;

select distinct sqlstr1 into : rule_code4 separated by ' + ' from llpa
where rownum >= 300 and rownum < 400;

quit;

proc sql;
update nl1
set llpa_sum = llpa_sum + &rule_code1 + &rule_code2 + &rule_code3 + &rule_code4;
quit;
%mend llpa;



/*Possible transactions offered under EverReady and their conditional new term based on the adjusted 
  remaining term of the loan - Cashout Same PI was removed from the transactions*/

data tran;                      
input TRANSACTION $ 1-19	REM_TERM	NEW_TERM; 
cards;           
RATE TERM REFINANCE 360 360
RATE TERM REFINANCE 300 360
RATE TERM REFINANCE 240 240
RATE TERM REFINANCE 180 180
RATE TERM REFINANCE 120 120
;
run; 

/*Connect to Eclipse/ ECR/ RENPU and pull necessary data for pricing*/

PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW='MRDM$101' PATH="RENPU") ;
/*To get product codes and their ENG_PROD_CD's with product features for pricing*/

CREATE table PROD1 AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT
P.PROD_CD,
P.ENG_PROD_CD,
P.PROD_NAME,
P.AM_TYP,
P.BE_RATIO,
P.CHAN_CD,
P.MORT_TYP,
P.FIRST_ADJ,
P.FIRST_INC,
P.SEC_ADJ,
P.SEC_INC,
CASE WHEN P.AM_TYP = 'A' AND P.MARGIN IS NULL THEN M1.MARGIN ELSE P.MARGIN END AS MARGIN,
CASE WHEN P.AM_TYP = 'A' AND P.INDX IS NULL THEN M1.INDX ELSE P.INDX END AS INDX,
CASE WHEN P.AM_TYP = 'A' AND P.LIFE_CAP IS NULL THEN M1.LIFE_CAP ELSE P.LIFE_CAP END AS LIFE_CAP
FROM
(
SELECT
B.PROD_CD,
B.ENG_PROD_CD,
A.PROD_NAME,
CASE WHEN A.PROD_TYPE = 'FIX' THEN 'F' ELSE 'A' END AS AM_TYP,
A.BE_RATIO,
A.CHAN_CD,
A.MORT_TYP,
A.FIRST_ADJ,
A.FIRST_INC,
A.SEC_ADJ,
A.SEC_INC,
M.MARGIN,
M.INDX,
M.LIFE_CAP
FROM PUB.PRODUCT@DTRPTP A,
(
SELECT DISTINCT
PROD_CD
,ENG_PROD_CD
FROM PUB.PT_T_ENG_PRODUCT@DTRPTP
where ENTITY_CD = 'NEWCO'
) B,
(
SELECT DISTINCT PROD_CD, MARGIN, INDX, LIFE_CAP FROM
PUB.RATEINDX@DTRPTP A
WHERE A.RATE_NO = (SELECT MAX(RATE_NO) FROM PUB.PT_T_RATEHIST@DTRPTP WHERE RATE_NO < 1000000)
AND ENTITY_CD = 'NEWCO' AND CHAN_CD = 'RETL'
) M
WHERE A.PROD_CD = B.PROD_CD
AND A.PROD_CD = M.PROD_CD(+)
AND A.ENTITY_CD = 'NEWCO' AND A.CHAN_CD = 'RETL'
) P,
(
SELECT DISTINCT PROD_CD, MARGIN, INDX, LIFE_CAP FROM
PUB.RATEINDX@DTRPTP A
WHERE A.RATE_NO = (SELECT MAX(RATE_NO) FROM PUB.PT_T_RATEHIST@DTRPTP WHERE RATE_NO < 1000000)
AND ENTITY_CD = 'NEWCO' AND CHAN_CD = 'RETL'
) M1
WHERE P.ENG_PROD_CD = M1.PROD_CD (+)
)
;

/*To get all LLPA's from Eclipse*/                                    
CREATE table LLPA AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT ROWNUM, SEQ, SQLSTR1
FROM
(
SELECT
SEQ,
'CASE WHEN ' || SQLSTR || ' THEN ' || POINTS || ' ELSE 0 END' AS SQLSTR1
FROM PUB.RULES_RATE_BUMP@DTRPTP
WHERE EFFEND > SYSDATE
AND (POINTS > -5 AND POINTS < 5)
ORDER BY SEQ
)
);

/*To get most recent Ratesheet*/ 
CREATE table RATESHEET1 AS
                        SELECT  DISTINCT *
                        FROM CONNECTION TO CONN1
(
SELECT
PROD_CD,
TIER_CD,
RATE_DT,
POINTS,
MIN(RATE) AS RATE
FROM
(
SELECT
RH2.PROD_CD,
RH2.TIER_CD,
RH2.RATE_DT as RATE_DT,
RH2.RATE,
RH2.PTS_090 AS POINTS
/*CASE WHEN RH2.PROD_CD='5/1C' OR PROD_CD = 'B14' then RH2.PTS_045 else RH2.PTS_005 end as POINTS*/
FROM PUB.PT_T_RATEHIST@DTRPTP RH2
WHERE  	RH2.RATE_NO = (SELECT  MAX(RAT2.RATE_NO) FROM  PUB.PT_T_RATEHIST@DTRPTP RAT2  WHERE RAT2.RATE_NO < 1000000)
and RH2.ENTITY_CD='NEWCO' AND RH2.CHAN_CD='RETL'
AND RH2.SCHAN_CD IN ('NT', 'NTHCTL')
ORDER BY RH2.PROD_CD,
RH2.RATE)
GROUP BY PROD_CD, TIER_CD, RATE_DT, POINTS
ORDER BY PROD_CD, POINTS
);

/*To get all Products for Batch Pricing from CSRG*/
CREATE table PROD_PRICED AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT * FROM CSRG_PROD.BATCH_PRICING_PRODUCTS
);

/*To get all FHA insurance factors*/
CREATE table FHA_MI1 AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT DISTINCT *
FROM
(
SELECT
MIFACT_RISK_BASED_ID,
RATE_TYPE,
PROD_CD,
UFMIP_FACT,
MI_FACT,
LTV_FROM,
CASE WHEN LTV_TO >= 120 THEN 10000 ELSE LTV_TO END AS LTV_TO,
LOANAMT_FROM,
LOANAMT_TO,
ENDORSEMENT_DATE_FROM,
ENDORSEMENT_DATE_TO
from PUB.PT_T_RISK_BASED_MIP@DTRPTP
where EXP_DATE is null
and RATE_TYPE in ('D', 'A')
and ltv_to >= 80
and prod_cd in ('15FH', '30FH')
)
order by RATE_TYPE
);

/*To get active loans from ECR*/
CREATE table LOANS1 AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT
L.ASSET_OWNER || '-' || L.ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
L.UNPAID_BALANCE,
L.INTEREST_RATE,
L.PRINCIPAL_AND_INTEREST,

L.CURRENT_PROPERTY_VALUE,
L.CURRENT_PROPERTY_METHOD,


L.PROP_STATE AS STATE,
CASE WHEN LIM.CONFORMING_ONE_UNIT_LOAN_LIMIT IS NULL THEN &l_limit ELSE LIM.CONFORMING_ONE_UNIT_LOAN_LIMIT END AS LOAN_LIMIT,
0 AS UPB_2,
L.ORIGINATION_DATE,
L.INVESTOR_FULL_NAME,
L.RATE_TYPE,
L.REMAINING_TERM,
L.PRODUCT_TYPE_CODE,
L.MI_AMOUNT AS CUR_MI,
L.MI_ENDORSEMENT_DATE AS END_DT,
'A' AS MI_TYPE
FROM
CSRG_PROD.LOAN_MASTER L,
PUB.ZIP_MSA_XREF_MV@DTRPTP LIM

WHERE L.ZIP = LIM.ZIP_CODE(+)
AND (L.GMAC_OWNED_FLAG = 'Y' OR L.ASSOCIATION_CODE = '013')
AND L.LIEN_POSITION ='01'
AND (L.LOAN_STATUS_CODE = '1' OR L.LOAN_STATUS_CODE = '9')
/*
AND L.ACCOUNT_NUMBER IN 

(
'0602225099',
'0179229109'
)
*/
);
DISCONNECT FROM CONN1;
QUIT;

/*CREATE NECESSARY BACTH PRICING RELEVANT TABLES FROM PULLED DATA*/

/*Remove I/O loans and loand with CPV = 0 from potfolio loans and calculate Adjusted remaining term*/
PROC SQL;
CREATE TABLE LOANS AS SELECT A.*,
CASE WHEN ROUNDZ(REMAINING_TERM,60) < 120 THEN 120
WHEN ROUNDZ(REMAINING_TERM,60) > 280 THEN 360
ELSE ROUNDZ(REMAINING_TERM,60) END AS AD_TERM,

CASE WHEN STATE IN ('HI', 'AK') AND (UNPAID_BALANCE > &l_limit_h) THEN 'JUMB' 
WHEN STATE NOT IN ('HI', 'AK') AND (UNPAID_BALANCE > &l_limit) THEN 'JUMB'
ELSE 'CONF' END AS TIER_CD,

CASE WHEN CLTV NE LTV THEN UPB_2 ELSE 0 END AS SUBFI_AMT,
&cred_score AS CRED_SCORE
FROM 
(
	SELECT
	ACCOUNT_NUMBER,
	ROUND(UNPAID_BALANCE,1) AS UNPAID_BALANCE,
	ROUND(CURRENT_PROPERTY_VALUE,1) AS CURRENT_PROPERTY_VALUE,
	ROUND(PRINCIPAL_AND_INTEREST,1) AS PRINCIPAL_AND_INTEREST,
	CASE WHEN CURRENT_PROPERTY_VALUE > 0 THEN ROUND(UNPAID_BALANCE/CURRENT_PROPERTY_VALUE * 100,.01) 
			  ELSE 0 END AS LTV,
	CASE WHEN CURRENT_PROPERTY_VALUE > 0 THEN ROUND((UNPAID_BALANCE + UPB_2)/CURRENT_PROPERTY_VALUE * 100,.01) 
			  ELSE 0 END AS CLTV,
	CASE WHEN (UNPAID_BALANCE * INTEREST_RATE/12)/PRINCIPAL_AND_INTEREST > 0.95 THEN REMAINING_TERM
	ELSE ROUND(MORT(UNPAID_BALANCE, PRINCIPAL_AND_INTEREST, INTEREST_RATE/12, .),1) END AS REMAINING_TERM,
	INTEREST_RATE,
	INVESTOR_FULL_NAME,
	CURRENT_PROPERTY_METHOD,
	ORIGINATION_DATE,
	STATE,
	UPB_2,
	RATE_TYPE,
	LOAN_LIMIT,
	PRODUCT_TYPE_CODE,
	CUR_MI,
	END_DT,
	MI_TYPE
	FROM LOANS1
	WHERE CURRENT_PROPERTY_VALUE > 0 /*for missing AVMs*/
	/*AND (UNPAID_BALANCE * INTEREST_RATE/12)/PRINCIPAL_AND_INTEREST < 0.95 remove IO loans*/
) AS A;

/*Remove uwanted products from Product table*/
CREATE TABLE PROD AS SELECT
A.*,
B.PROD_CD,
B.AM_TYP,
B.BE_RATIO,
B.CHAN_CD,
B.MORT_TYP,
CASE WHEN A.TIER_CD = 'JUMB' AND A.PROG_TYP = 'SSTD' THEN 'JUMB' ELSE 'CONF' END AS PROC_TYP,
B.FIRST_ADJ,
B.FIRST_INC,
B.SEC_ADJ,
B.SEC_INC,
B.MARGIN,
B.INDX,
B.LIFE_CAP
FROM PROD_PRICED AS A, PROD1 AS B
WHERE A.ENG_PROD_CD = B.ENG_PROD_CD;

/*Get only relevant rates from RATESHEET*/
CREATE TABLE RATESHEET  AS SELECT DISTINCT
A.ENG_PROD_CD,
B.PROD_CD,
B.TIER_CD,
B.RATE_DT,
B.RATE,
B.POINTS
FROM PROD AS A LEFT JOIN  
(SELECT T.ENG_PROD_CD FROM PROD AS T, RATESHEET1 AS U WHERE T.ENG_PROD_CD = U.PROD_CD) AS C
ON A.ENG_PROD_CD = C.ENG_PROD_CD,
RATESHEET1 AS B
WHERE ((C.ENG_PROD_CD <> '' AND A.ENG_PROD_CD = B.PROD_CD AND A.TIER_CD = B.TIER_CD)
OR (C.ENG_PROD_CD = '' AND A.PROD_CD = B.PROD_CD AND A.TIER_CD = B.TIER_CD))
;
QUIT;

PROC SQL;
CREATE TABLE NL AS SELECT DISTINCT
A.ACCOUNT_NUMBER,
A.UNPAID_BALANCE,
CASE WHEN B.TRANSACTION = 'CASHOUT 80 LTV' THEN 
CASE WHEN A.UNPAID_BALANCE > A.LOAN_LIMIT THEN ROUND((.80 * A.CURRENT_PROPERTY_VALUE),1) ELSE
CASE WHEN A.UNPAID_BALANCE > &l_limit THEN MIN(ROUND(.80 * A.CURRENT_PROPERTY_VALUE,1), A.LOAN_LIMIT) 
ELSE MIN(ROUND(.80 * A.CURRENT_PROPERTY_VALUE,1), &l_limit)
END END
ELSE A.UNPAID_BALANCE END AS LOAN_AMT,
A.CURRENT_PROPERTY_VALUE,
A.PRINCIPAL_AND_INTEREST,
CASE WHEN B.TRANSACTION = 'CASHOUT 80 LTV' THEN 80 ELSE A.LTV END AS LTV,
CASE WHEN B.TRANSACTION = 'CASHOUT 80 LTV' THEN ((A.CLTV - A.LTV) + 80) ELSE A.CLTV END AS CLTV,
A.REMAINING_TERM,
A.INTEREST_RATE,
A.INVESTOR_FULL_NAME,
A.UPB_2,
A.ORIGINATION_DATE,
A.STATE,
A.RATE_TYPE,
A.LOAN_LIMIT,
A.AD_TERM,
A.TIER_CD,
A.SUBFI_AMT,
A.CRED_SCORE, 
A.PRODUCT_TYPE_CODE,
A.CUR_MI,
A.END_DT,
A.MI_TYPE,
A.CURRENT_PROPERTY_METHOD,
B.TRANSACTION,
B.NEW_TERM AS NEW_TERM
FROM LOANS AS A, TRAN B
WHERE A.AD_TERM = B.REM_TERM
AND (B.TRANSACTION NE 'CASHOUT 80 LTV' OR A.LTV <= 79.99); 
QUIT;

/*Using product table assign products to each transaction*/
PROC SQL;
CREATE TABLE NL1 AS SELECT
L.*,
CASE WHEN M.UFMIP_FACT  = . THEN 0 ELSE M.UFMIP_FACT END AS UFMIP,
CASE WHEN M.MI_FACT  = . THEN 0 ELSE M.MI_FACT END AS MIP,
CASE WHEN CURRENT_PROPERTY_METHOD = 'IPV' THEN
CASE WHEN NEW_PRODUCT LIKE ('%HASP%') THEN MONOTONIC() + 20000000 ELSE MONOTONIC() END 
ELSE 
CASE WHEN NEW_PRODUCT LIKE ('%HASP%') THEN MONOTONIC() ELSE MONOTONIC() + 20000000 END
END AS CNT
FROM
(
SELECT
A.ACCOUNT_NUMBER,
A.TRANSACTION,
TRIM(PUT(A.NEW_TERM/12,2.)) || ' YR ' || B.PROD_DESCRIPT AS NEW_PRODUCT,
A.PRINCIPAL_AND_INTEREST,
A.REMAINING_TERM AS ADJ_REMAINING_TERM,
A.CURRENT_PROPERTY_VALUE,
A.CLTV,
A.CRED_SCORE,
A.UNPAID_BALANCE,
A.LOAN_AMT,
A.LTV,
A.STATE AS P_STATE,
A.SUBFI_AMT,
A.AD_TERM,
A.NEW_TERM,
A.NEW_TERM AS TERM,
A.INVESTOR_FULL_NAME,
A.LOAN_LIMIT,
CASE WHEN A.TRANSACTION = 'CASHOUT 80 LTV' THEN 'CO' ELSE 'LC' END AS REFI_CD,
A.PRODUCT_TYPE_CODE,
A.CUR_MI,
A.END_DT,
A.MI_TYPE,
A.CURRENT_PROPERTY_METHOD,
B.*,
.25 AS LLPA_SUM
FROM NL AS A, PROD AS B
WHERE (A.NEW_TERM >= B.MIN_TERM AND A.NEW_TERM <= B.MAX_TERM)
AND (A.LTV >= B.MIN_LTV AND A.LTV <= B.MAX_LTV)
AND (B.INVESTOR_RESTRICT = '' OR B.INVESTOR_RESTRICT = A.INVESTOR_FULL_NAME)
AND (B.ORG_DATE_RESTRICT = . OR B.ORG_DATE_RESTRICT >= A.ORIGINATION_DATE)
AND A.TIER_CD = B.TIER_CD
AND (B.TRANS_RESTRICT = '' OR (B.TRANS_RESTRICT = 'CO' AND A.TRANSACTION NE 'CASHOUT 80 LTV')) 
) AS L LEFT JOIN FHA_MI1 AS M ON (L.PROD_CD = M.PROD_CD AND L.MI_TYPE = M.RATE_TYPE)
WHERE 
(
M.PROD_CD = ''
OR
(M.RATE_TYPE = 'A' AND L.LTV >= M.LTV_FROM AND L.LTV <= M.LTV_TO AND 
 L.LOAN_AMT >= M.LOANAMT_FROM AND L.LOAN_AMT <= M.LOANAMT_TO)
OR
(M.RATE_TYPE = 'D' AND L.LTV >= M.LTV_FROM AND L.LTV <= M.LTV_TO AND 
 L.LOAN_AMT >= M.LOANAMT_FROM AND L.LOAN_AMT <= M.LOANAMT_TO AND
 (M.ENDORSEMENT_DATE_FROM = . AND L.END_DT <= M.ENDORSEMENT_DATE_TO OR
  M.ENDORSEMENT_DATE_TO = . AND L.END_DT >= M.ENDORSEMENT_DATE_FROM) 
)
)
ORDER BY ACCOUNT_NUMBER, NEW_PRODUCT;

&hi_bal; 
&jumbo;

&va;

QUIT;

%llpa;

proc sql;
delete from nl1 where new_product like ('%ARM%');
quit;

PROC RANK DATA=NL1 OUT=NL2 DESCENDING TIES=LOW;
BY ACCOUNT_NUMBER;
VAR CNT;
RANKS P_RANK;
RUN;

PROC SQL;
DELETE FROM NL2 WHERE P_RANK = 2;
QUIT;

PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM + 0;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC1 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_0_BPS,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_0_BPS
FROM RATES;
QUIT;


PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM + 2;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC2 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_200_BPS,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_200_BPS
FROM RATES;
QUIT;

PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM  - 1;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC3 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_100_BPS,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_100_BPS
FROM RATES;
QUIT;

PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM - .5;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC4 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_50_BPS,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_50_BPS
FROM RATES;
QUIT;

PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM - 2.5;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC5 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_200_BPSL,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_200_BPSL
FROM RATES;
QUIT;

PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM + 1;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC6 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_100_BPSL,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_100_BPSL
FROM RATES;
QUIT;

PROC SQL;
UPDATE NL2
SET LLPA_SUM = LLPA_SUM + .5;
QUIT;

proc sql;
create table rates as
select a.*, 
b.rate,
b.netpoints
from nl2 as a
left join
(
select distinct
b1.account_number,
b1.transaction,
b1.eng_prod_cd,
b1.term,
B1.llpa_sum,
case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then (r2.points + b1.llpa_sum) else
case when (r2.points + b1.llpa_sum) < 0 then 0 else (r2.points + b1.llpa_sum) end end as netpoints,
r2.rate as rate
from nl2 as b1, ratesheet as r2, 
(select eng_prod_cd, max(points) as max_points from ratesheet group by eng_prod_cd) as m2
where m2.eng_prod_cd = b1.eng_prod_cd
and r2.eng_prod_cd = b1.eng_prod_cd
and 
(r2.points + b1.llpa_sum) >= 
						(case when b1.eng_prod_cd in ('051', '050', 'V44', 'V50') and b1.ltv > 97.75 then
							case when (m2.max_points + b1.llpa_sum) < -1.5 then (m2.max_points + b1.llpa_sum)
							  else -1.5 end						 
						 else	
							case when (m2.max_points + b1.llpa_sum) < 0 then (m2.max_points + b1.llpa_sum)
							  when (m2.max_points + b1.llpa_sum) < 1 then 0 else 0 end
						end)

group by b1.account_number, b1.transaction, b1.eng_prod_cd, b1.term, b1.llpa_sum
having ((r2.points + b1.llpa_sum) = min((r2.points + b1.llpa_sum)))
) as b
on( a.account_number = b.account_number
and a.transaction = b.transaction
and a.eng_prod_cd = b.eng_prod_cd
and a.term = b.term);
quit;


PROC SQL;
CREATE TABLE SC7 AS SELECT 
ACCOUNT_NUMBER,
RATE AS RATE_50_BPSL,
ROUND(PRINCIPAL_AND_INTEREST - MORT(UNPAID_BALANCE,.,RATE/1200,NEW_TERM), 1) AS PI_BEN_50_BPSL
FROM RATES;
QUIT;


PROC SQL;
CREATE TABLE X AS SELECT
TRIM(SCAN(A.ACCOUNT_NUMBER,1,'-')) AS ASSET_OWNER,
TRIM(SCAN(A.ACCOUNT_NUMBER,2,'-')) AS ACCOUNT_NUMBER length = 12,
B.RATE_200_BPSL,
C.RATE_100_BPSL,
D.RATE_50_BPSL,
A.RATE_0_BPS,
G.RATE_50_BPS,
F.RATE_100_BPS,
E.RATE_200_BPS,
B.PI_BEN_200_BPSL,
C.PI_BEN_100_BPSL,
D.PI_BEN_50_BPSL,
A.PI_BEN_0_BPS,
G.PI_BEN_50_BPS,
F.PI_BEN_100_BPS,
E.PI_BEN_200_BPS
FROM SC1 AS A, 
SC2 AS E, 
SC3 AS F, 
SC4 AS G,
SC5 AS B,
SC6 AS C,
SC7 AS D
WHERE A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
AND A.ACCOUNT_NUMBER = C.ACCOUNT_NUMBER
AND A.ACCOUNT_NUMBER = D.ACCOUNT_NUMBER
AND A.ACCOUNT_NUMBER = E.ACCOUNT_NUMBER
AND A.ACCOUNT_NUMBER = F.ACCOUNT_NUMBER
AND A.ACCOUNT_NUMBER = G.ACCOUNT_NUMBER;
QUIT;

libname csrg  oracle user=CSRG_PROD password='mrdm$101' path=RENPU schema=CSRG_PROD;

PROC SQL;
drop table csrg.POINTS_SECNARIO_DATA;
create table csrg.POINTS_SECNARIO_DATA as select * from X;
quit;

libname csrg clear;
