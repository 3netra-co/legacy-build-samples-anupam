PROC SQL;      
		CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW="mrdm$101"  PATH=RENPU) ;
			
			CREATE table MatchEclipsePreApps  AS
      		SELECT  *
      		FROM CONNECTION TO CONN1
          		( 
select cast(substr(a.loan_no,3,10) as char(10)) as formattedloan,
       replace(c.ss_no,'-') as ss_no,
       a.sname_cd,
	   a.sloc_cd,
	   a.refi_purp,
	   a.purp_cd,
	   c.user_id,
	   cast(case when a.loan_no = j.loan_no then 'ILEAD'
            when a.sname_cd IN ('GMAC','IDBL1','IDBL2','IDBL3','NEWEX','PMGMT','RETEN','REO') THEN 'OTHER'
            else 'CALL' end as varchar2(5)) as loan_source_retired,
       cast(case when a.loan_no = g.loan_no then 'ILEAD'
            when a.sname_cd IN ('GMAC','IDBL1','IDBL2','IDBL3','NEWEX','PMGMT','RETEN','REO') THEN 'OTHER'
            else 'CALL' end as varchar2(5)) as loan_source,
       /*cast(case when a.loan_no = k.loan_number and k.web_app_id is not null then 'ILEAD'
            when a.sname_cd IN ('GMAC','IDBL1','IDBL2','IDBL3','NEWEX','PMGMT','RETEN','REO') THEN 'OTHER'
            else 'CALL' end as varchar2(5)) as loan_source2,*/
	   cast(case when a.purp_cd = 'P' and a.prod_cd not in ('2EQ','2IO','2FX','FRDM') THEN 'PURCHASE'
            when a.purp_cd = 'O' and a.refi_purp = 'Purchase' THEN 'PURCHASE'
			when a.purp_cd = 'P' and a.prod_cd is null then 'PURCHASE'
            else 'REFINANCE' end as varchar2(9)) as purpose_description,
	   cast(case when a.prod_cd is null THEN 'UNKNOWN' else a.prod_cd end as varchar2(7)) as Product_code,
	   cast(case when f.prod_name is null THEN 'UNKNOWN' else prod_name end as varchar2(25)) as Product_Descrip,
	   trunc(a.create_dt) as create_date,
       trunc(b.create_dt) as setup_date,
	   trunc(e.fund_dt) as fund_date,
	   cast(substr(H_ZIP,1,5) as char(5)) as ZIP5,
       cast(Upper(H_NUMB || ' ' || H_STREET || ' ' || H_APT) as varchar2(48)) AS MAILING_ADDRESS,
       Upper(c.FNAME) as FIRST_NAME,
       Upper(c.LNAME) as LAST_NAME,
	   a.client_status,
       d.stat_desc,
       cast(CASE WHEN a.ENTITY_CD in ('DT','GMAC','INC','CADIRECT') AND a.CHAN_CD = 'RETL' THEN 'Ditech'
            WHEN a.ENTITY_CD in ('MTL') AND a.CHAN_CD = 'RETL' THEN 'Direct'
            WHEN a.ENTITY_CD in ('MTL') AND a.CHAN_CD = 'RET2' THEN 'NCCC'
            else 'OTHER' END as varchar2(6)) AS Channel,
       CASE 
WHEN (a.CLIENT_STATUS = '8' and trunc(e.fund_dt) < trunc(sysdate) and b.create_dt is not null) OR
                 (a.CLIENT_STATUS IN ('7','8A','8B','S','H') AND e.fund_dt is not null and b.create_dt is not null) then 'FUNDED'
WHEN a.CLIENT_STATUS IN ('Q','Q1') AND b.create_dt is not null AND i.decline_cd is not null THEN 'DECLINED'
WHEN a.CLIENT_STATUS IN ('Q','Q1') AND b.create_dt is not null THEN 'CANCELLED'
WHEN (a.CLIENT_STATUS = '8' AND trunc(e.fund_dt) >= trunc(sysdate) and b.create_dt is not null) OR
                 a.CLIENT_STATUS NOT IN ('Q','Q1','7','8','8A','8B','S','H') and b.create_dt is not null AND
                 (a.CLIENT_STATUS NOT IN ('A','B','C','D','L') OR b.create_dt is not null) THEN 'PIPELINE'
WHEN b.create_dt is not null THEN 'FULLAPP'
WHEN b.create_dt is null THEN 'PREAPP'
ELSE '' END AS MEASURE_TYPE,
1 AS CNT,
CASE WHEN a.MORT_TYP = 'VA' THEN a.LOAN_AMT + a.MIFIN_AMT
                      WHEN h.MIP_FIN = 'Y' THEN a.LOAN_AMT + h.MIP_UFTOT
                      else a.LOAN_AMT end as AMOUNT
from Pub.LOANS@DTRPTP a  left join Pub.loan_setup@DTRPTP b on (a.loan_no = b.loan_no)
                         left join Pub.borrower@DTRPTP c on (a.loan_no = c.loan_no)
                         left join Pub.ClientStatus@DTRPTP d on (a.client_status = d.stat_cd)
                         left join Pub.Dates@DTRPTP e on (a.loan_no = e.loan_no)
				         left join Pub.Product@DTRPTP f on (a.prod_cd = f.prod_cd)
                                                       and (a.chan_cd = f.chan_cd)
										               and (a.entity_cd = f.entity_cd)
				         left join Pub.pt_t_ileads_xml@DTRPTP g on (a.loan_no = g.loan_no)
				         left join PUB.FHADATA@DTRPTP h on (a.loan_no = h.loan_no)
				         left join PUB.HMDACRA@DTRPTP i on (a.loan_no = i.loan_no)
				         left join csrg_prod.ilead_prod j on (a.loan_no = j.loan_no)
						 /*left join CLFDM_OWNER.BASE_KPI_DETAILS@CLFDMP k on (a.loan_no = k.loan_number)*/
WHERE trunc(a.CREATE_DT) between to_date('07/17/2009','mm/dd/yyyy') and
                                 trunc(sysdate) - 1
AND c.BORR_NO = '1'
            );

QUIT;
PROC SQL;      
		CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW="mrdm$101"  PATH=RENPU) ;

	CREATE table Calls2  AS
      		SELECT  *
      		FROM CONNECTION TO CONN1
          		( SELECT *
                  FROM CLFDM_OWNER.VW_CALLDATA_VDN_PROD@CLFDMP
                  WHERE trunc(RDATE) between to_date('07/17/2009','mm/dd/yyyy') and trunc(sysdate) - 1
				  
);
QUIT;

PROC SQL;      
		CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW="mrdm$101"  PATH=RENPU) ;
			
			CREATE table SOLICITATION_HISTORY  AS
      		SELECT  *
      		FROM CONNECTION TO CONN1
          		( 
SELECT DISTINCT A.*,
CU.FIRST_NAME,
CU.LAST_NAME,
AD.ZIP,
AD.ADDRESS_LINE_1
FROM
(
SELECT
S.ACCOUNT_NUMBER,
S.CUSTOMER_ID,
S.CUST_PRIM_SSN,
S.CUST_SCND_SSN,
S.CAMPAIGN_KEY,
S.CELL_KEY,
S.CAMPAIGN_ID,
CL.CELL_DESCRIPTION,
C.CAMPAIGN_DESCRIPTION,
CASE WHEN S.DISPOSITION_DATE IS NULL THEN C.CAMPAIGN_START_DATE ELSE S.DISPOSITION_DATE END AS CAMPAIGN_START_DATE,
CASE WHEN S.DISPOSITION_DATE IS NULL THEN C.CAMPAIGN_START_DATE + 60 ELSE S.DISPOSITION_DATE + 60 END AS CAMPAIGN_END_DATE,
S.DISPOSITION_KEY,
S.CONTROL_GROUP_INDICATOR
FROM CSRG_PROD.SOLICITATION_HISTORY S,
(SELECT ACCOUNT_NUMBER, CELL_KEY, MIN(DISPOSITION_DATE) AS D_DATE 
 FROM SOLICITATION_HISTORY WHERE DISPOSITION_KEY <> 9999 GROUP BY ACCOUNT_NUMBER, CELL_KEY) DUP,
CSRG_PROD.CAMPAIGN_T C,
CSRG_PROD.CAMPAIGN_CELL_T CL,
CSRG_PROD.CAMPAIGN_LIST L
WHERE S.CELL_KEY = CL.CELL_KEY
AND S.CELL_KEY = DUP.CELL_KEY
AND 
S.ACCOUNT_NUMBER = DUP.ACCOUNT_NUMBER
AND (S.DISPOSITION_DATE IS NULL OR S.DISPOSITION_DATE = DUP.D_DATE)
AND S.CELL_KEY = L.CELL_KEY
AND CL.CAMPAIGN_KEY = C.CAMPAIGN_KEY
AND S.DISPOSITION_KEY <> 9999
) A,
ECR_CORE.CUSTOMER@GMEDAPP CU,
ECR_CORE.ADDRESS@GMEDAPP AD

WHERE
A.CUSTOMER_ID = CU.CUSTOMER_ID
AND CU.CURRENT_ADDRESS_ID = AD.ADDRESS_ID
);

			CREATE table CAMPAIGN_LIST  AS
      		SELECT  *
      		FROM CONNECTION TO CONN1
          		( 
SELECT * FROM CSRG_PROD.CAMPAIGN_LIST
);
QUIT;

PROC SQL;
 
                        CONNECT TO ORACLE AS CONN1 (USER=CSRG_PROD ORAPW='MRDM$101' PATH="RENPU") ;
                                    
                                    CREATE table PAYOFFS AS
                        SELECT  *
                        FROM CONNECTION TO CONN1
(
SELECT SSN,
ACCOUNT_NUMBER,
PAYOFF_DATE,
'NON RETAINED' AS GRP
FROM CSRG_PROD.PAYOFFS 
WHERE PRIMARY_IND = 'Y'
);
QUIT;

proc sql;
create table matchup_solic_otherprod as
select distinct
	ecr.*, seg.account_number, 
	seg.cust_prim_ssn as cust_prim_ssn,seg.customer_id as customer_id,seg.campaign_key as campaign_id,
	seg.cell_key, seg.control_group_indicator as control_group_ind,
	'' as customer_id_prim, '' as first_name_prim,
datdif(datepart(seg.campaign_start_date), datepart(ecr.create_date), 'act/act') as diff
from matcheclipsepreapps ecr join solicitation_history seg 
on (ecr.ss_no = seg.cust_prim_ssn)
where ecr.ss_no not in ('000000000','999999999','111111111')
and ecr.ss_no is not missing
and datepart(ecr.create_date) >= datepart(seg.campaign_start_date) 
and datepart(ecr.create_date) <= datepart(seg.campaign_end_date);
quit;

proc sql;
create table matchup_solic_otherprod_111 as
select distinct
	ecr.*, seg.account_number,
	seg.cust_prim_ssn as cust_prim_ssn,
	seg.customer_id as customer_id,
	seg.campaign_key as campaign_id,
	seg.cell_key,
	seg.control_group_indicator as control_group_ind,
	'' as customer_id_prim, '' as first_name_prim,
datdif(datepart(seg.campaign_start_date), datepart(ecr.create_date), 'act/act') as diff
from matcheclipsepreapps ecr,
     solicitation_history seg 
where ecr.Zip5 = seg.ZIP
and spedis(ecr.last_name, seg.last_name) < 20 
and spedis(ecr.mailing_address, seg.address_line_1) < 20
and ecr.ss_no in ('000000000','999999999','111111111')
and ecr.ss_no is not missing
and datepart(ecr.create_date) >= datepart(seg.campaign_start_date) 
and datepart(ecr.create_date) <= datepart(seg.campaign_end_date);
quit;

proc sql;
insert into matchup_solic_otherprod
select * from matchup_solic_otherprod_111;
quit;

PROC SQL;
CREATE TABLE A AS SELECT A.* FROM MATCHUP_SOLIC_OTHERPROD AS A,
(SELECT FORMATTEDLOAN, MIN(DIFF) AS DIFF FROM MATCHUP_SOLIC_OTHERPROD GROUP BY FORMATTEDLOAN) AS B
WHERE A.FORMATTEDLOAN = B.FORMATTEDLOAN
AND A.DIFF = B.DIFF
ORDER BY A.FORMATTEDLOAN, A.CELL_KEY;
QUIT;

PROC RANK DATA=A OUT=MATCHUP_SOLIC_OTHERPROD1 TIES=LOW;
BY FORMATTEDLOAN;
VAR CELL_KEY;
RANKS RANK;
RUN;

proc sql;
create table paidoff as
select distinct
	seg.account_number, 
	seg.cell_key, 
	seg.control_group_indicator as control_group_ind,
	'' as customer_id_prim, 
	'' as first_name_prim,
case when payoff_date = . then . else datdif(datepart(seg.campaign_start_date), datepart(po.payoff_date), 'act/act') end as diff
from solicitation_history seg join payoffs po
on (seg.account_number = po.account_number)
where
datepart(po.payoff_date) >= datepart(seg.campaign_start_date) 
and datepart(po.payoff_date) <= datepart(seg.campaign_end_date)
;
quit;

PROC RANK DATA=PAIDOFF OUT=PAIDOFF1 TIES=LOW;
BY ACCOUNT_NUMBER;
VAR CELL_KEY;
RANKS RANK;
RUN;

PROC SQL;
CREATE TABLE SOLICITATION_HISTORY1 AS SELECT A.*, C.MEASURE_TYPE,
CASE WHEN C.ACCOUNT_NUMBER = '' THEN 
	CASE WHEN B.ACCOUNT_NUMBER = '' THEN 'NON RESPONDER RETAINED' 
	ELSE 'NON RESPONDER NOT RETAINED' END
ELSE
CASE WHEN B.ACCOUNT_NUMBER = '' THEN 'RESPONDER RETAINED' 
ELSE 'RESPONDER NOT RETAINED' END 
END AS GRP
FROM SOLICITATION_HISTORY AS A LEFT JOIN PAIDOFF1 AS B 
ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.CELL_KEY = B.CELL_KEY)
LEFT JOIN MATCHUP_SOLIC_OTHERPROD1 AS C
ON (A.ACCOUNT_NUMBER = C.ACCOUNT_NUMBER AND A.CELL_KEY = C.CELL_KEY)
WHERE (C.RANK = . OR C.RANK = 1);
QUIT;

PROC SQL;
CREATE TABLE CALLS1 AS SELECT
A.VDN,
A.INBOUND_CALLS,
A.ACD_CALLS,
B.CELL_KEY,
B.CAMPAIGN_KEY,
A.RDATE,
B.cs_DATE,
B.ce_DATE,
datdif(datepart(b.cs_date), datepart(a.rdate), 'act/act') as diff
from calls2 as a, 
(
	select t.*, 
	s.cs_date,
	s.ce_date
	from campaign_list as t, 
	(
	select cell_key, 
	min(campaign_start_date) as cs_date format = datetime20.,
	min(campaign_end_date) as ce_date format = datetime20.
	from solicitation_history where control_group_indicator = 'N' group by cell_key
	) s
	where t.cell_key = s.cell_key
)
as b
where input(b.vdn,10.) = a.vdn
and datepart(a.rdate) >= datepart(b.cs_date) 
and datepart(a.rdate) <= datepart(b.ce_date);
quit;


PROC SQL;
CREATE TABLE A1 AS SELECT A.* FROM CALLS1 AS A,
(SELECT VDN, RDATE, MIN(DIFF) AS DIFF FROM CALLS1 GROUP BY VDN, RDATE) AS B
WHERE A.VDN = B.VDN
AND A.RDATE = B.RDATE
AND A.DIFF = B.DIFF
ORDER BY A.VDN, A.RDATE, A.CELL_KEY;
QUIT;

PROC RANK DATA=A1 OUT=CALLS TIES=LOW;
BY VDN RDATE;
VAR CELL_KEY;
RANKS RANK;
RUN;

PROC SQL;
CREATE TABLE VDNAPPS1 AS SELECT
A.*,
B.CELL_KEY,
B.SOURCE_CODE,
B.cs_DATE,
B.ce_DATE,
datdif(datepart(b.cs_date), datepart(a.create_date), 'act/act') as diff
from matcheclipsepreapps as a, 
(
	select t.*, 
	s.cs_date,
	s.ce_date
	from campaign_list as t, 
	(
	select cell_key, 
	min(campaign_start_date) as cs_date format = datetime20.,
	min(campaign_end_date) as ce_date format = datetime20.
	from solicitation_history where control_group_indicator = 'N' group by cell_key
	) s
	where t.cell_key = s.cell_key
)
as b
where b.source_code = a.sname_cd
and datepart(a.create_date) >= datepart(b.cs_date) 
and datepart(a.create_date) <= datepart(b.ce_date);
quit;


PROC SQL;
CREATE TABLE A2 AS SELECT A.* FROM VDNAPPS1 AS A,
(SELECT FORMATTEDLOAN, MIN(DIFF) AS DIFF FROM VDNAPPS1 GROUP BY FORMATTEDLOAN) AS B
WHERE A.FORMATTEDLOAN = B.FORMATTEDLOAN
AND A.DIFF = B.DIFF
ORDER BY A.FORMATTEDLOAN, A.CELL_KEY;
QUIT;

PROC RANK DATA=A2 OUT=VDNAPPS TIES=LOW;
BY FORMATTEDLOAN;
VAR CELL_KEY;
RANKS RANK;
RUN;

PROC SQL;
CREATE TABLE METRIC1 AS SELECT DISTINCT
CELL_KEY,
'SSN' AS MATCH_TYPE,
LOAN_SOURCE AS SOURCE_TYPE FORMAT = $50. LENGTH = 50,
MEASURE_TYPE LENGTH = 20,
PURPOSE_DESCRIPTION AS PURPOSE,
PRODUCT_DESCRIP AS PRODUCT_DESCRIPTION,
SUM(CASE WHEN CONTROL_GROUP_IND = 'N' THEN CNT ELSE 0 END) AS TEST_CNT,
SUM(CASE WHEN CONTROL_GROUP_IND <> 'N' THEN CNT ELSE 0 END) AS CONTROL_CNT,
SUM(CASE WHEN CONTROL_GROUP_IND = 'N' THEN AMOUNT ELSE 0 END) AS TEST_AMOUNT,
SUM(CASE WHEN CONTROL_GROUP_IND <> 'N' THEN AMOUNT ELSE 0 END) AS CONTROL_AMOUNT
FROM MATCHUP_SOLIC_OTHERPROD1 
WHERE RANK = 1
GROUP BY 
CELL_KEY,
MATCH_TYPE,
LOAN_SOURCE,
MEASURE_TYPE,
PURPOSE_DESCRIPTION,
PRODUCT_DESCRIP;
QUIT;

PROC SQL;
CREATE TABLE METRIC2 AS SELECT DISTINCT
CELL_KEY,
'VDN' AS MATCH_TYPE,
LOAN_SOURCE AS SOURCE_TYPE FORMAT = $50. LENGTH = 50,
MEASURE_TYPE LENGTH = 20,
PURPOSE_DESCRIPTION AS PURPOSE,
PRODUCT_DESCRIP AS PRODUCT_DESCRIPTION,
SUM(CNT) AS TEST_CNT,
0 AS CONTROL_CNT,
SUM(AMOUNT) AS TEST_AMOUNT,
0 AS CONTROL_AMOUNT
FROM VDNAPPS 
WHERE RANK = 1
GROUP BY 
CELL_KEY,
MATCH_TYPE,
LOAN_SOURCE,
MEASURE_TYPE,
PURPOSE_DESCRIPTION,
PRODUCT_DESCRIP;
QUIT;

PROC SQL;
CREATE TABLE METRIC3 AS SELECT DISTINCT
CELL_KEY,
'VDN' AS MATCH_TYPE,
'CALL' AS SOURCE_TYPE FORMAT = $50. LENGTH = 50,
'INBOUND CALLS' AS MEASURE_TYPE LENGTH = 20,
'' AS PURPOSE,
'' AS PRODUCT_DESCRIPTION,
SUM(INBOUND_CALLS) AS TEST_CNT,
0 AS CONTROL_CNT,
0 AS TEST_AMOUNT,
0 AS CONTROL_AMOUNT
FROM CALLS 
WHERE RANK = 1
GROUP BY 
CELL_KEY,
MATCH_TYPE,
SOURCE_TYPE,
MEASURE_TYPE,
PURPOSE,
PRODUCT_DESCRIPTION;
QUIT;


PROC SQL;
CREATE TABLE METRIC4 AS SELECT DISTINCT
CELL_KEY,
'VDN' AS MATCH_TYPE,
'CALL' AS SOURCE_TYPE FORMAT = $50. LENGTH = 50,
'ANSWERED CALLS' AS MEASURE_TYPE LENGTH = 20,
'' AS PURPOSE,
'' AS PRODUCT_DESCRIPTION,
SUM(ACD_CALLS) AS TEST_CNT,
0 AS CONTROL_CNT,
0 AS TEST_AMOUNT,
0 AS CONTROL_AMOUNT
FROM CALLS 
WHERE RANK = 1
GROUP BY 
CELL_KEY,
MATCH_TYPE,
SOURCE_TYPE,
MEASURE_TYPE,
PURPOSE,
PRODUCT_DESCRIPTION;
QUIT;

PROC SQL;
CREATE TABLE METRIC5 AS SELECT DISTINCT
CELL_KEY,
'RTN' AS MATCH_TYPE,
CASE WHEN MEASURE_TYPE IN ('FUNDED', 'PIPELINE') AND GRP = 'RESPONDER NOT RETAINED' THEN 'RESPONDER RETAINED' ELSE TRIM(GRP) END AS SOURCE_TYPE FORMAT = $50. LENGTH = 50,
MEASURE_TYPE LENGTH = 20,
'' AS PURPOSE,
'' AS PRODUCT_DESCRIPTION,
SUM(CASE WHEN CONTROL_GROUP_INDICATOR = 'N' THEN 1 ELSE 0 END) AS TEST_CNT,
SUM(CASE WHEN CONTROL_GROUP_INDICATOR <> 'N' THEN 1 ELSE 0 END) AS CONTROL_CNT,
0 AS TEST_AMOUNT,
0 AS CONTROL_AMOUNT
FROM SOLICITATION_HISTORY1 
GROUP BY 
CELL_KEY,
MATCH_TYPE,
SOURCE_TYPE,
MEASURE_TYPE,
PURPOSE,
PRODUCT_DESCRIPTION;
QUIT;

PROC SQL;
CREATE TABLE METRIC6 AS SELECT DISTINCT
CELL_KEY,
'' AS MATCH_TYPE,
'' AS SOURCE_TYPE FORMAT = $50. LENGTH = 50,
'QUANTITY' AS MEASURE_TYPE LENGTH = 20,
'' AS PURPOSE,
'' AS PRODUCT_DESCRIPTION,
SUM(CASE WHEN CONTROL_GROUP_INDICATOR = 'N' THEN 1 ELSE 0 END) AS TEST_CNT,
SUM(CASE WHEN CONTROL_GROUP_INDICATOR <> 'N' THEN 1 ELSE 0 END) AS CONTROL_CNT
FROM SOLICITATION_HISTORY
GROUP BY 
CELL_KEY,
MATCH_TYPE,
SOURCE_TYPE,
MEASURE_TYPE,
PURPOSE,
PRODUCT_DESCRIPTION;
QUIT;

PROC APPEND BASE  = METRIC1 DATA = METRIC2; RUN;
PROC APPEND BASE  = METRIC1 DATA = METRIC3; RUN;
PROC APPEND BASE  = METRIC1 DATA = METRIC4; RUN;
PROC APPEND BASE  = METRIC1 DATA = METRIC5 force; RUN;


PROC SQL;
CREATE TABLE METRIC7 AS SELECT
A.*,
CASE WHEN B.TEST_CNT = . THEN 0 ELSE B.TEST_CNT END AS TEST_QTY,
CASE WHEN B.CONTROL_CNT = . THEN 0 ELSE B.CONTROL_CNT END AS CONTROL_QTY
FROM METRIC1 AS A LEFT JOIN METRIC6 AS B
ON
(
A.CELL_KEY = B.CELL_KEY
)
ORDER BY A.CELL_KEY;
QUIT;

PROC SQL;
CREATE TABLE CAMPAIGN_REPORTING_DATA AS SELECT
YEAR(DATEPART(A.CAMPAIGN_START_DATE)) AS CAMPAIGN_YR,
A.CELL_KEY,
A.CELL_DESCRIPTION,
A.CAMPAIGN_KEY,
A.CAMPAIGN_DESCRIPTION,
A.CAMPAIGN_START_DATE,
A.CAMPAIGN_END_DATE,
A.PRODUCT_PROMOTED,
A.CHANNEL,
A.CAMPAIGN_NAME,
A.PROG_GROUP,
A.PROG_TYPE,
A.VDN,
A.SOURCE_CODE,
A.SOURCE_LOC,
A.COST_PER_SOLICITATION,
A.MARGIN_ON_FUND,
B.MATCH_TYPE,
B.SOURCE_TYPE,
B.MEASURE_TYPE,
B.PURPOSE,
B.PRODUCT_DESCRIPTION,
B.TEST_CNT,
B.TEST_AMOUNT,
B.CONTROL_CNT,
B.CONTROL_AMOUNT,
CASE WHEN B.TEST_QTY = . THEN C.TEST_CNT ELSE B.TEST_QTY END AS TEST_QTY,
CASE WHEN B.CONTROL_QTY = . THEN C.CONTROL_CNT ELSE B.CONTROL_QTY END AS CONTROL_QTY,
CASE WHEN B.TEST_QTY > 0 OR C.TEST_CNT > 0 THEN 1 ELSE 0 END AS TEST_FLAG,
CASE WHEN B.CONTROL_QTY >0 OR C.CONTROL_CNT > 0 THEN 1 ELSE 0 END AS CONTROL_FLAG
FROM CAMPAIGN_LIST AS A LEFT JOIN METRIC7 AS B
ON A.CELL_KEY = B.CELL_KEY
LEFT JOIN METRIC6 AS C
ON A.CELL_KEY = C.CELL_KEY
ORDER BY CAMPAIGN_NAME, CELL_KEY;

DELETE FROM CAMPAIGN_REPORTING_DATA WHERE TEST_FLAG = 0 AND CONTROL_FLAG = 0;
QUIT;



libname csrg  oracle user=CSRG_PROD password='mrdm$101' path=RENPU schema=CSRG_PROD;

PROC SQL;
drop table csrg.CAMPAIGN_REPORTING_DATA;
create table csrg.CAMPAIGN_REPORTING_DATA as select * from CAMPAIGN_REPORTING_DATA;
quit;

libname csrg clear;


