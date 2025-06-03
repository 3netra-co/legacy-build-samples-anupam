/
--------------------------------------------------------------------------------------------------------------------------------------------
-- MANUAL RUN:  Manually run the job
exec SP_DPL_01_VELOCIFY_EXTRACT;

--------------------------------------------------------------------------------------------------------------------------------------------
-- View stored procedure compile errors
select * from all_errors where name='SP_DPL_01_VELOCIFY_EXTRACT';
select * from all_source where name='SP_DPL_01_VELOCIFY_EXTRACT';


/
--------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE STORED PROCEDURE:
CREATE OR REPLACE PROCEDURE SP_DPL_01_VELOCIFY_EXTRACT AS

	date_now date :=trunc(SYSDATE);

BEGIN

execute immediate 'ALTER SESSION ENABLE PARALLEL DML';

if trunc(sysdate)='4/04/2018' then
	SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','** RESTARTING AND SKIPPING SOME STEPS **',null,null);
	COMMIT;
	goto RESTART_HERE;
end if;

----------------------------------------------------------------------------------------------
-- Step 1:  Extract data from SDR Velocify tables
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','START: SP_DPL_01_VELOCIFY_EXTRACT',null,null);
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','1 - TRUNC/INSERT into DPL_VELOCIFY_EXTRACT',null,null);

Execute Immediate ('TRUNCATE TABLE DPL_VELOCIFY_EXTRACT');
COMMIT;

INSERT /*+  append nologging*/
	INTO DPL_VELOCIFY_EXTRACT
	(	VELOCIFY_LEAD_ID,
		FIRST_NAME,
		LAST_NAME,
		EMAIL,
		BORROWER_ADDRESS,
		BORROWER_CITY,
		BORROWER_STATE,
		BORROWER_ZIP,
		COBORROWER_STATE,
		DAY_PHONE,
		EVENING_PHONE,
		MOBILE_PHONE,
		SMS_PHONE,
		PROPERTY_ADDRESS,
		PROPERTY_CITY,
		PROPERTY_STATE,
		PROPERTY_ZIP,
		BEST_CONTACT_METHOD,
		LOAN_AMOUNT,
		CAMPAIGN_DESCRIPTION,
		PROPERTY_FOUND,
		VDN,
		SOURCE_CODE_ORIG,
		LEAD_SOURCE,
		ENTITY,
		CHANNEL,
		UTM_CAMPAIGN,
		CURRENT_ACCOUNT_NO,
		DO_NOT_CALL,
		DO_NOT_EMAIL,
		DO_NOT_MAIL,
		CUSTOMER_CALLING_NUMBER,
		COBORROWER_FIRST_NAME,
		COBORROWER_LAST_NAME,
		INTENDED_PROPERTY_USE,
		LOAN_TYPE,
		LOAN_PURPOSE,
		CURRENT_NEED_SITUATION,
		LENDING_TREE_QFORM_NAME,
		REFERRAL_SOURCE,
		LEAD_CREATE_DATE,
		ENCOMPASS_GUID,
		ENCOMPASS_LOAN_NUMBER,
		PROPERTY_TYPE,
		PURPOSE_OF_REFINANCE,
		NEED_PURCHASE_REALTOR,
		NEED_SELLING_REALTOR,
		ENCOMPASS_LOAN_STATUS,
		LEAD_STATUS,
		AGENT_NAME,
		AGENT_WORK_PHONE,
		AGENT_NMLS,
		AGENT_EMAIL,
		FILTER_ID,
		DO_NOT_EMAIL_CODE,
		BORROWER_SSN,
		CURRENT_MILESTONE,
		VELOCIFY_CAMPAIGN_NAME,
		UNIT_KEY_DESC,
		--CONTACT_MADE,
		CONCIERGE_FLAG,
		CONCIERGE_AGENT,
		FIRST_CONTACT_ATTEMPT_DATE,
		TOTAL_CONTACT_ATTEMPTS,
		FIRST_CONTACT_ATTEMPT_TYPE,
		FIRST_CONTACT_MADE_DATE,
		FIRST_CONTACT_MADE_TYPE,
		LAST_ACTION_TAKEN,
		ATTRIBUTED_ENCOMPASS_GUID,
		ATTRIBUTED_ENCOMPASS_LOAN_NO,
		EXISTING_CUSTOMER_YN,
		AGENT_FAX_NUMBER,
		MILITARY_STATUS,
		FIRST_VA_LOAN,
		CASH_OUT_AMOUNT,
		CHANNEL_GROUP,
		BORROWER_BIRTH_DATE,
		COBORROWER_SSN,
		FIRST_TIME_HOMEBUYER,
		VELOCIFY_CAMPAIGN_NAME_ORIG,
		DATE_ADDED,
		AGENT_KEY,
		LAST_ACTION_DATE)
	(SELECT /*+ DRIVING_SITE(BASICINFO) first_rows
			parallel(BASICINFO, 2)
			*/
		BASICINFO.VELOCIFY_LEAD_KEY VELOCIFY_LEAD_ID,
		upper(replace(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.FIRST_NAME),'\','/')) FIRST_NAME, -- '
		upper(replace(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.last_NAME),'\','/'))  LAST_NAME, --'
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.email)  EMAIL,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BORROWERINFO.BORROWER_ADDRESS) AS borrower_address,
        SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BORROWERINFO.BORROWER_CITY)    AS BORROWER_CITY,
        SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BORROWERINFO.BORROWER_STATE)    AS BORROWER_STATE,
        SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BORROWERINFO.BORROWER_ZIPCODE) AS  BORROWER_ZIP,
        SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(COBORROWERINFO.BORROWER_STATE) AS  COBORROWER_STATE,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.DAY_PHONE) DAY_PHONE,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.EVENING_PHONE) EVENING_PHONE,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.MOBILE_PHONE) MOBILE_PHONE,
		BASICINFO.SMS_PHONE /*SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.SMS_PHONE)*/ SMS_PHONE,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.PROPERTY_ADDRESS) PROPERTY_ADDRESS,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.PROPERTY_CITY) PROPERTY_CITY,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.PROPERTY_STATE) PROPERTY_STATE,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BASICINFO.PROPERTY_ZIP) PROPERTY_ZIP,
		BASICINFO.BEST_CONTACT_METHOD,
		BASICINFO.LOAN_AMOUNT,
		MARKETINGINFO.CAMPAIGN_DESCR CAMPAIGN_DESCRIPTION,
		BASICINFO.PROPERTY_FOUND,
		Marketinginfo.Vdn,
		MARKETINGINFO.SOURCE_CODE,
--    	case when MARKETINGINFO.SOURCE_CODE = 'CHAT' THEN 'ditechWeb' else MARKETINGINFO.SOURCE_CODE end source_code,
--		case
--			when BASICINFO.ENCOMPASS_GUID is null then nvl(MARKETINGINFO.LEAD_SOURCE,marketinginfo.source_code)
--			else marketinginfo.lead_source
--		end as LEAD_SOURCE,
		case when filter_id in (896022,896021) then 'LENDINGTREESFR' else marketinginfo.lead_source end ,
		'DITECH' as ENTITY,
		--upper(MARKETINGINFO.ENTITY),
		/*case when agent.agent_email is not null then
			CASE
				WHEN LOWER(AGENT.AGENT_EMAIL) LIKE '%gtoriginations.com'
				and nvl(UNIT_NAME.UNIT_KEY_DESC,'x') <> '.Concierge'
					THEN 'GREENTREE'
				when MARKETINGINFO.SOURCE_CODE = 'ETUSA' then 'GREENTREE'
				ELSE 'DITECH'
			END
		else upper(marketinginfo.entity)
		end ENTITY,*/
		CASE
			WHEN MARKETINGINFO.CHANNEL IS NULL AND BASICINFO.LEAD_CREATE_DATE <= to_date('10/10/2014','mm/dd/yyyy') THEN 'Direct'
			WHEN CHANNEL='Retail' then 'Retail'
			WHEN CHANNEL='Retention' then 'Retention'
			ELSE 'Direct' --MARKETINGINFO.CHANNEL
		END as CHANNEL,
		MARKETINGINFO.UTM_CAMPAIGN,
		--MARKETINGINFO.SERVICING_ACCOUNT_NO CURRENT_ACCOUNT_NO,
		substr(NVL(LOANINFO.CURRENT_ACCOUNT_NO,MARKETINGINFO.SERVICING_ACCOUNT_NO),1,90) as CURRENT_ACCOUNT_NO,
		MARKETINGINFO.DO_NOT_CALL,
		MARKETINGINFO.DO_NOT_EMAIL,
		MARKETINGINFO.DO_NOT_MAIL,
		MARKETINGINFO.CUSTOMER_CALLING_NO,
        SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(COBORROWERINFO.BORROWER_FIRST_NAME) AS COBORROWER_FIRST_NAME,
        SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(COBORROWERINFO.BORROWER_LAST_NAME) AS COBORROWER_LAST_NAME,
		PROPERTYINFO.INTENDED_PROPERTY_USE,
		LOANINFO.LOAN_TYPE,
		LOANINFO.LOAN_PURPOSE,
		LOANINFO.CURRENT_NEED_SITUATION,
		LT.LOAN_APPLICATION_ID LENDING_TREE_QFORM_NAME,
		case  -- remove non-digit referral source values which are the ilead values.  It is used to join to ILEAD_ID on ilead tables and errors if it has non-digits.
			when BASICINFO.REF_ID is null then null
			when length(trim(translate(BASICINFO.REF_ID,'1234567890 ',' ')))>0 then null
			--when BASICINFO.REF_ID >= 100000000 then null
			else BASICINFO.REF_ID
		end as REFERRAL_SOURCE,
		BASICINFO.LEAD_CREATE_DATE LEAD_CREATE_DATE,
		upper(replace(replace(BASICINFO.ENCOMPASS_GUID,'{',''),'}','')) ENCOMPASS_GUID,
		TO_NUMBER(BASICINFO.ENCOMPASS_LOAN_NUMBER) ENCOMPASS_LOAN_NUMBER,
		BASICINFO.PROPERTY_TYPE,
		LOANINFO.PURPOSE_OF_REFINANCE,
		BASICINFO.PURCHASE_REALTOR_NEED NEED_PURCHASE_REALTOR,
		BASICINFO.SELLING_REALTOR_NEED NEED_SELLING_REALTOR,
		BASICINFO.ENCOMPASS_LOAN_STATUS,
		STATUS.STATUS_DESC LEAD_STATUS,
		AGENT.AGENT_FIRST_NAME|| ' ' || AGENT.AGENT_LAST_NAME AGENT_NAME,
		AGENT.AGENT_WORK_PHONE,
		AGENT.CUSTOM1 AGENT_NMLS,
		AGENT.AGENT_EMAIL,
		LT.FILTER_ID,
		--NULL,
		CASE WHEN BASICINFO.ENCOMPASS_GUID IS NOT NULL THEN 'CRM' ELSE NULL END,
		SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(BORROWERINFO.BORROWER_ssn),
		MILESTONE.MILESTONE_TYPE_DESC CURRENT_MILESTONE,
		CAMPAIGN.CAMPAIGN_DESC VELOCIFY_CAMPAIGN_NAME,
		nvl(UNIT_NAME.UNIT_KEY_DESC,'Unknown'),
		--CASE WHEN CON.LEAD_KEY IS NULL THEN 'N' ELSE 'Y' END CONTACT_MADE,
		null concierge_flag,
		null concierge_agent,
		null first_contact_attempt_date,
		null total_contact_attempts,
		null first_contact_attempt_type,
		null first_contact_made_date,
		null first_contact_made_type,
		act.ACTION_TYPE_DESC LAST_ACTION_TAKEN,
		null attributed_encompass_guid,
		null attributed_encompass_loan_no,
		UPPER(MARKETINGINFO.EXISTING_CUSTOMER_YN),
		AGENT.agent_fax_phone AGENT_FAX_NUMBER,
		basicinfo.military_status,
		basicinfo.first_va_loan,
		basicinfo.CASH_OUT_AMOUNT,
		Case
			When
				CASE
					WHEN MARKETINGINFO.CHANNEL IS NULL AND BASICINFO.LEAD_CREATE_DATE <= to_date('10/10/2014','mm/dd/yyyy') THEN 'Direct'
					ELSE MARKETINGINFO.CHANNEL
				END
				in ('Direct','Retail') then 'Acquisition'
			else 'Retention'
		end as CHANNEL_GROUP,
		borrowerinfo.BORROWER_BIRTH_DATE,
		Sdr_Delta.Pkg_Crypto.Decrypt@Renpup2sdrp(Coborrowerinfo.Borrower_Ssn),
		Basicinfo.First_Time_Homebuyer,
    	CAMPAIGN.CAMPAIGN_DESC,
    	NULL as DATE_ADDED,
    	Basicinfo.AGENT_KEY,
		BASICINFO.LAST_ACTION_DATE

	FROM
		SDR_CORE.VELOCIFY_LEAD_DLY_BASICINFO@renpup2sdrp BASICINFO
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_BORROWERINFO@renpup2sdrp BORROWERINFO
			ON BASICINFO.VELOCIFY_LEAD_KEY = BORROWERINFO.LEAD_KEY
			AND BORROWERINFO.BORROWER_NO = 1 --PRIMARY BORROWER
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_BORROWERINFO@renpup2sdrp COBORROWERINFO
			ON BASICINFO.VELOCIFY_LEAD_KEY = COBORROWERINFO.LEAD_KEY
			AND COBORROWERINFO.BORROWER_NO = 2 --CO-BORROWER
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_LOANINFO@renpup2sdrp LOANINFO
			ON BASICINFO.VELOCIFY_LEAD_KEY = LOANINFO.LEAD_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_PROPERTYINFO@renpup2sdrp PROPERTYINFO
			ON BASICINFO.VELOCIFY_LEAD_KEY = PROPERTYINFO.LEAD_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_MARKETNGINFO@renpup2sdrp MARKETINGINFO
			ON BASICINFO.VELOCIFY_LEAD_KEY = MARKETINGINFO.LEAD_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_STATUS@renpup2sdrp STATUS
			ON STATUS.STATUS_KEY = BASICINFO.STATUS_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_AGENT_DLY_PROFILE@renpup2sdrp AGENT
			ON AGENT.AGENT_KEY = BASICINFO.AGENT_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_LEAD_DLY_LTDETAILS@renpup2sdrp LT
			ON BASICINFO.VELOCIFY_LEAD_KEY = LT.LEAD_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_MILESTONE_NA_TYPE@renpup2sdrp MILESTONE
			ON BASICINFO.MILESTONE_TYPE_KEY = MILESTONE.MILESTONE_TYPE_KEY
		LEFT JOIN SDR_CORE.VELOCIFY_CAMPAIGN_DLY_PROFILE@RENPUP2SDRP CAMPAIGN
			ON BASICINFO.CAMPAIGN_KEY = CAMPAIGN.CAMPAIGN_KEY
		LEFT JOIN (select /*+ parallel(a, 2) */
				distinct a.unit_key, a.unit_key_desc
			from SDR_CORE.VELOCIFY_LEAD_DLY_LOG@RENPUP2SDRP a
			inner join
				(select /*+ parallel(x, 2) */
					unit_key, max(log_date) max_log_date
				from SDR_CORE.VELOCIFY_LEAD_DLY_LOG@RENPUP2SDRP x
				group by unit_key) b
			on a.unit_key = b.unit_key and a.log_date = b.max_log_date) UNIT_NAME
			ON AGENT.UNIT_KEY = UNIT_NAME.UNIT_KEY
		left join SDR_CORE.VELOCIFY_ACTION_DLY_TYPE@renpup2sdrp ACT
			ON BASICINFO.LAST_ACTION_TYPE_KEY = ACT.ACTION_TYPE_KEY
	WHERE
	not (BASICINFO.STATUS_KEY in (91,100) and BASICINFO.LAST_ACTION_TYPE_KEY is null )
	);

commit;




----------------------------------------------------------------------------------------------
-- Step 1a: Delete any dups on VELOCIFY_LEAD_ID
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','1a - Delete dups on VELOCIFY_LEAD_ID',null,null);
delete
from DPL_VELOCIFY_EXTRACT
where VELOCIFY_LEAD_ID in
	(	select VELOCIFY_LEAD_ID
		from  DPL_VELOCIFY_EXTRACT
		group by VELOCIFY_LEAD_ID
		having COUNT(*) >1	);
commit;



----------------------------------------------------------------------------------------------
-- Step 1b: Delete bogus test records
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','1b - Delete test records',null,null);
delete /*+ parallel(a, 2) */
from DPL_VELOCIFY_EXTRACT a
where LEAD_CREATE_DATE >= trunc(sysdate)
	or nvl(upper(LEAD_STATUS),'x') = 'TEST'
	or nvl(upper(LEAD_SOURCE), 'x') = 'TESTLEADSOURCE'
	or ( LEAD_SOURCE is null and AGENT_NAME = 'Cindy Jackson' )  	-- Per email from Cindy Jackson on 10/23/2017
  or AGENT_NAME = 'Cindy Jackson'
	or VELOCIFY_CAMPAIGN_NAME='dtd-NOI'							-- Per email from Cindy Jackson on 10/23/2017
	or ( LEAD_STATUS= 'Batch Remove' and LAST_ACTION_TAKEN is null )
	or ( LEAD_STATUS= 'Suppressed' and LAST_ACTION_TAKEN is null )
	or LEAD_STATUS IN ('TEST','TEST-SENT TO ENCOMPASS')
	or nvl(upper(FIRST_NAME),'x') = 'TESTSTETER'
	or upper(FIRST_NAME) like '%TESTY%'
	or upper(FIRST_NAME) = 'DITECH'
	or ( upper(FIRST_NAME) like '%TEST%' and upper(LAST_NAME) like '%TEST%' )
	or upper(LAST_NAME) = 'TESTCASE'
	or upper(LAST_NAME) in ('UTM','UTM2', 'TEST')
	or nvl(upper(VELOCIFY_CAMPAIGN_NAME),'x') = 'DTD-TEST_DITECHWEB'
	or nvl(upper(VELOCIFY_CAMPAIGN_NAME),'x') = 'TEST-LOW'
	or nvl(upper(LEAD_SOURCE), 'x') = 'TESTLEADSOURCE'
	or AGENT_NAME = 'Jack Nixon'
	or upper(EMAIL) like '%MOCKDEPLOY%'
	or upper(EMAIL) like '%MOCKLEAD%'
	or upper(EMAIL) like '%USERTESTING%'
	or upper(EMAIL) like '%NOSERVER.NET%'
	or upper(EMAIL) like '%TESTCASE%'
	or upper(EMAIL) like '%@TEST.COM'
  or upper(EMAIL) like '%@TAVANT.%'
  
;
commit;

--GOTO END_POINT;


----------------------------------------------------------------------------------------------
-- Step 2: Set PRIOR_ACCOUNT_NO  by validating CURRENT_ACCOUNT_NO against MDM_ACCOUNT_NUMBER
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','2 - Update/Fix PRIOR_ACCOUNT_NO and REFERRAL_SOURCE',null,null);

merge /*+ parallel(a, 2) parallel(b, 2)  */
into DPL_VELOCIFY_EXTRACT a
using (
	select /*+ parallel(v, 2) parallel(man, 2) */
		v.VELOCIFY_LEAD_ID,
		v.CURRENT_ACCOUNT_NO,
		man.ACCOUNT_NUMBER
	from DPL_VELOCIFY_EXTRACT v
		left join  MDM_ACCOUNT_NUMBER man on trim(ltrim(v.CURRENT_ACCOUNT_NO,'0'))=man.AN
		--on trim(ltrim(v.CURRENT_ACCOUNT_NO,'0')) = trim(ltrim(man.AN,'0')) -- reserved on 11162018 by AW -- DP Failed 11162018
	where v.CURRENT_ACCOUNT_NO is not null
	) b
on (a.VELOCIFY_LEAD_ID = b.VELOCIFY_LEAD_ID)
when matched then update set
	a.PRIOR_ACCOUNT_NO = b.ACCOUNT_NUMBER,
	a.PRIOR_ACCOUNT_NO_SOURCE =
		case
			when b.CURRENT_ACCOUNT_NO=b.ACCOUNT_NUMBER then 'LEAD'
			when b.ACCOUNT_NUMBER is not null then 'LEAD-FIXED'
			else null
		end;
commit;

-- Delete leads that were added due to system issues where multiple bogus leads were created for the same account on the same day
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','2a - Delete bogus leads (>15/account/day)',null,null);

DELETE /*+ parallel(a, 2) */FROM DPL_VELOCIFY_EXTRACT a
WHERE exists
	(	SELECT /*+ parallel(a, 2) parallel(b, 2) */
			1
		FROM
			(	SELECT /*+ parallel(c, 2) */
					CURRENT_ACCOUNT_NO,TRUNC(LEAD_CREATE_DATE) as LEAD_CREATE_DATE
				FROM DPL_VELOCIFY_EXTRACT c
				WHERE CURRENT_ACCOUNT_NO is not null and ENCOMPASS_LOAN_NUMBER is null
				GROUP BY CURRENT_ACCOUNT_NO,TRUNC(LEAD_CREATE_DATE)
				HAVING count(*)>10
			) b
		where a.CURRENT_ACCOUNT_NO=b.CURRENT_ACCOUNT_NO and TRUNC(a.LEAD_CREATE_DATE)=b.LEAD_CREATE_DATE
	);
commit;

  -- 7/30/2015 BTS
-- If day phone, evening phone or mobile phone is used over 20 times, delete those numbers from DPL_VELOCIFY_EXTRACT.

-- The idea is that most of these are NOT the borrower numbers and we shouldn't use them.

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','3a - Remove bogus DAY_PHONE',null,null);

update /*+ parallel(v, 2) */
	DPL_VELOCIFY_EXTRACT v
set v.day_phone=null
where v.day_phone is not null
	and exists
	( 	select /*+ parallel(b, 2) */
			phone
		from
			(	select  /*+ parallel(a, 2) */
			 		distinct day_phone as phone
				from DPL_VELOCIFY_EXTRACT a
				where day_phone is not null
				group by day_phone
				having count(*)  > 20
			) b
		where v.day_phone=b.phone
	);
commit;

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','3b - Remove bogus EVENING_PHONE',null,null);

update /*+ parallel(v, 2) */
DPL_VELOCIFY_EXTRACT v
set v.evening_phone=null
where v.evening_phone is not null
	and exists
	( 	select /*+ parallel(b, 2) */
			phone
		from
			(	select  /*+ parallel(a, 2) */
			 		distinct evening_phone as phone
				from DPL_VELOCIFY_EXTRACT a
				where evening_phone is not null
				group by evening_phone
				having count(*) > 20
			) b
		where v.evening_phone=b.phone
	);
commit;

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','3c - Remove bogus MOBILE_PHONE',null,null);

update /*+ parallel(v, 2) */
DPL_VELOCIFY_EXTRACT v
set v.mobile_phone=null
where v.mobile_phone is not null
	and exists
	( 	select /*+ parallel(b, 2) */
			phone
		from
			(	select  /*+ parallel(a, 2) */
			 		distinct mobile_phone as phone
				from DPL_VELOCIFY_EXTRACT a
				where mobile_phone is not null
				group by mobile_phone
				having count(*)  > 20
			) b
		where v.mobile_phone=b.phone
	);
commit;

<<RESTART_HERE>>

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','3d - LM_VALIDIATE SSN/EMAIL/PHONE',null,null);
update /*+ parallel(v, 2) */
DPL_VELOCIFY_EXTRACT v
set
	v.BORROWER_SSN		= LM_CLEAN.LM_VALIDATE_SSN(v.BORROWER_SSN),
	v.COBORROWER_SSN 	= LM_CLEAN.LM_VALIDATE_SSN(v.COBORROWER_SSN),
	v.EMAIL				= LM_CLEAN.LM_VALIDATE_EMAIL(v.EMAIL),
	v.DAY_PHONE			= LM_CLEAN.LM_VALIDATE_PHONE(v.DAY_PHONE),
	v.MOBILE_PHONE 		= LM_CLEAN.LM_VALIDATE_PHONE(v.MOBILE_PHONE),
	v.EVENING_PHONE		= LM_CLEAN.LM_VALIDATE_PHONE(v.EVENING_PHONE)
where not (v.BORROWER_SSN is null and v.COBORROWER_SSN is null and v.EMAIL is null and v.DAY_PHONE is null and v.MOBILE_PHONE is null and v.EVENING_PHONE is null)
;
commit;

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','4 - TRUNC/INSERT into MDM_LEAD_LT',Null,Null);

-- Create table of Lending Tree-only leads..specific info only
-- ***  May not be currently used ****

Execute Immediate ('TRUNCATE TABLE MDM_LEAD_LT');
COMMIT;

INSERT /*+ append nologging */
	INTO MDM_LEAD_LT
	SELECT /*+ DRIVING_SITE(LTD) first_rows parallel(LTD, 2) */
		LTD.LEAD_KEY as CRM_ID,
		LTD.PRESENT_LTV,
		LTD.PRESENT_CLTV,
		LTD.PROPOSED_LTV,
		LTD.PROPOSED_CLTV,
		LTD.FICO,
		LTD.PRICING_TYPE,
		LTD.LOAN_APPLICATION_ID as LENDING_TREE_QFORM_NAME
	FROM SDR_CORE.VELOCIFY_LEAD_DLY_LTDETAILS@renpup2sdrp LTD

	WHERE LTD.FILTER_ID IS NOT NULL;

commit;

delete
from MDM_LEAD_LT a
where exists
	(	select CRM_ID
		from DPL_VELOCIFY_EXTRACT b
		where a.CRM_ID=b.velocify_lead_ID
			and b.vELOCIFY_CAMPAIGN_NAME = 'dtd-TEST_ditechWeb');
commit;


-- Check Email address for do-not-email conditions
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','7 - Do Not Email',Null,Null);

MERGE /*+ first_rows parallel(v, 2) parallel(v2, 2) */
	INTO DPL_VELOCIFY_EXTRACT V
USING
	(SELECT /*+ parallel(v1,2) */
		VELOCIFY_LEAD_ID,
		substr(email,1,instr(email,'^E')-1) EMAIL,
		SUBSTR(EMAIL,INSTR(email,'^E')+3,4) DO_NOT_EMAIL_CODE,
		'TRUE' DO_NOT_EMAIL
	FROM DPL_VELOCIFY_EXTRACT V1
	WHERE V1.email like '%^E%') V2
ON
(V.VELOCIFY_LEAD_ID = V2.VELOCIFY_LEAD_ID)
WHEN MATCHED THEN UPDATE
SET
	V.EMAIL = V2.EMAIL,
	V.DO_NOT_EMAIL_CODE = V2.DO_NOT_EMAIL_CODE,
	V.DO_NOT_EMAIL = V2.DO_NOT_EMAIL;

COMMIT;


update /*+ parallel(a,2) */
	DPL_VELOCIFY_EXTRACT a
set do_not_email  = 'TRUE'
where email is not null and email not like '%@%';

commit;

--
--SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','8 - TRUNC/INSERT into DB_LEAD_CONTACT_TYPES',Null,Null);
--COMMIT;
--
--
---- Get list of contact types
--Execute Immediate ('truncate table DB_LEAD_CONTACT_TYPES');
--COMMIT;
--
--insert into db_lead_Contact_types
--	(	log_Type_key,
--		log_type_desc,
--		count,
--		date_first_used
--	)
--	(	select /*+ DRIVING_SITE(a) first_rows parallel(a, 2) */
--			log_type_key,
--			log_type_desc,
--			count(*) as count,
--			min(log_date) as date_first_used
--		from sdr_core.velocify_lead_dly_log@renpup2sdrp a
--		group by log_type_key, log_type_desc);
--commit;
--
--update DB_LEAD_CONTACT_TYPES
--set CONTACT_ATTEMPT_FLAG='Y'
--	where log_type_key in ( 110,71,87,33,68,86,112,91,92,93,94,95,96,97,
--							101,98,99,100,103,102,88,89,105,106,107,108,109,
--							115,121,120,90,111,116,117,125,126
--							-- Added these 4 on 10/20
--							,128,130,132,131
--							-- added on 12/8
--							,137
--              				--  Added on 2/17/2017
--              				,146, 144, 67, 145, 143, 140, 139);
--
--update DB_LEAD_CONTACT_TYPES
--set CONTACT_MADE_FLAG='Y'
--	where log_type_key in ( 110,71,87,86,112,92,93,94,95,96,97,101,98,
--							99,100,103,102,88,89,105,106,107,108,109,115,121,
--							120,90,117,116,91
--							-- Added these 4 on 10/20
--							,128,130,132,131
--							--118,  118 removed on 4/29 after discussion with Tim.  Previously it counted as contact made, but not contact attempted.
--							-- added on 12/8
--							,137
--							-- added Export to Encompass on 12/11
--							,123
--              --  Added on 2/17/2017
--              -- Intentionally not add status 145 (Dialer - Customer Disconnected).
--              ,146, 144, 67,  143, 140, 139);
--
--update DB_LEAD_CONTACT_TYPES
--set AGENT_SENT_EMAIL='Y'
--	where log_type_key in ( 138 -- 'E-Mail Sent'
--							);
--
--commit;

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','9 - Set CONCIERGE_FLAG',Null,Null);

execute immediate 'truncate table DPL_TMP_CONCIERGE';
insert /* append nologging */
	into DPL_TMP_CONCIERGE
select
	LEAD_KEY, first_assign_distr_agent
	--,LOG_DATE, CONCIERGE_LOG_OBJECT_CNT
from
	(	select  /*+ DRIVING_SITE(a) first_rows parallel (a,8) */
			LEAD_KEY,
			first_assign_distr_agent,
--			LOG_DATE,
--			count(*) over (partition by LEAD_KEY) as CONCIERGE_LOG_OBJECT_CNT,
			rank() over (partition by LEAD_KEY order by LOG_DATE,rownum) as RANK
		from sdr_core.velocify_lead_dly_log@renpup2sdrp a
		where LOG_OBJECT like '%Concierge%'
	)
where RANK=1;
commit;
--create index DPL_TMP_CONCIERGE_X on DPL_TMP_CONCIERGE (LEAD_KEY);




MERGE /*+ first_rows parallel(v, 2) parallel(c, 2) */
	INTO DPL_VELOCIFY_EXTRACT V
USING
	DPL_TMP_CONCIERGE C
ON (V.VELOCIFY_LEAD_ID = C.LEAD_KEY)
WHEN MATCHED THEN UPDATE SET
	V.CONCIERGE_FLAG = 'Y',
	V.concierge_agent = C.FIRST_ASSIGN_DISTR_AGENT;

COMMIT;




SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','10 - Extract Contact Attempts',Null,Null);

--drop table DPL_TMP_VEL_CONTACT1;
--create table DPL_TMP_VEL_CONTACT1 as
execute immediate 'truncate table DPL_TMP_VEL_CONTACT1';
insert /* append nologging */ into DPL_TMP_VEL_CONTACT1
select /*+  parallel (b,8) */
	b.*,
-- Get contact attempts from Velocify log file
	-- Note the following log types do not count as attempts:
	--  138 Agent Email Sent
	--  149 Export to Blend
	case when log_type_key not in (138) then 1 else 0 end as NUM_ATTEMPTS,
	case when  log_type_key in ( 110,71,87,86,112,92,93,94,95,96,97,101,98,
					99,100,103,102,88,89,105,106,107,108,109,115,121,
					120,90,117,116,91
					-- Added these 4 on 10/20
					,128,130,132,131
					--118,  118 removed on 4/29 after discussion with Tim.  Previously it counted as contact made, but not contact attempted.
					-- added on 12/8First
					,137
					-- added Export to Encompass on 12/11
					,123
	              --  Added on 2/17/2017
	              -- Intentionally not add status 145 (Dialer - Customer Disconnected).
	              ,146, 144, 67,  143, 140, 139
	              ,149 -- Export to Blend
	              )
	              then 'Y' else 'N' end as CONTACT_MADE,
	case when log_type_key in (138,150) then 'Y' else 'N' end as AGENT_EMAIL_SENT
from
	( 	-- Must do this group by below because, incredibly, some leads have dup log records.
		select /*+ DRIVING_SITE(a) first_rows parallel (a,8) */
				a.LEAD_KEY,
				min(a.LEAD_KEY_SEQ) as LEAD_KEY_SEQ,
				a.LOG_TYPE_KEY,
				a.LOG_TYPE_DESC,
				a.LOG_DATE

			from
				sdr_core.velocify_lead_dly_log@renpup2sdrp a
			where log_type_key in ( 110,71,87,33,68,86,112,91,92,93,94,95,96,97,
							101,98,99,100,103,102,88,89,105,106,107,108,109,
							115,121,120,90,111,116,117,125,126
							-- Added these 4 on 10/20
							,128,130,132,131
							-- added on 12/8
							,137
			           		--  Added on 2/17/2017
			           		,146, 144, 67, 145, 143, 140, 139
			           		,123 -- Export to Encompass
			           		,138 -- Email Sent (does not count as Attempt or Contact)
			           		,149 -- Export to Blend
                    ,150
			           		)
			group by
				a.lead_key,
				a.log_type_key,
				a.LOG_TYPE_DESC,
				a.LOG_DATE
	) b;



-- Get contact attempts from Velocify DialIQ log

insert /* append */
	into DPL_TMP_VEL_CONTACT1
	(	LEAD_KEY,
		LEAD_KEY_SEQ,
		LOG_TYPE_KEY,
		LOG_TYPE_DESC,
		LOG_DATE,
		NUM_ATTEMPTS  )
select /*+ DRIVING_SITE(b) first_rows parallel (b,8) */
	b.lead_key,
	-99 as lead_key_seq,
	-99 as log_type_key,
	'DialIQ Outbound Call' as LOG_TYPE_DESC,
	min(b.CALL_STARTED) as LOG_DATE,
	0 as NUM_ATTEMPTS  -- count any and all dialer attempts as 1, but don't double count with Velocify log records...so see GREATEST(1, above)
from
	SDR_CORE.VELOCIFY_LEAD_DLY_DIALERLOG@renpup2sdrp b
group by b.lead_key;
commit;
--create index DPL_TMP_VEL_CONTACT1_X on DPL_TMP_VEL_CONTACT1 (lead_key,log_date);

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','11 - Find First Contact Date/Type',Null,Null);

-- Create First Contact Made by LEAD_KEY
--drop table DPL_TMP_VEL_CONTACT2;
--create table DPL_TMP_VEL_CONTACT2 as
execute immediate 'truncate table DPL_TMP_VEL_CONTACT2';
insert /* append nologging */ into DPL_TMP_VEL_CONTACT2
select  /*+ parallel(b, 2) */
	LEAD_KEY,
	LEAD_KEY_SEQ,
	LOG_DATE as FIRST_CONTACT_MADE_DATE,
    LOG_TYPE_DESC as FIRST_CONTACT_MADE_TYPE
from
	(	select /*+ parallel(a, 2) */
			LEAD_KEY,
			LEAD_KEY_SEQ,
			LOG_DATE,
			LOG_TYPE_DESC,
			rank() over (partition by LEAD_KEY order by LOG_DATE,rownum) as RANK
		from
			DPL_TMP_VEL_CONTACT1 a
		where
			CONTACT_MADE='Y'
	) b
where RANK=1
order by 1,2;
commit;


-- Create First Contact Made by LEAD_KEY
--create index DPL_TMP_VEL_CONTACT2A_X on DPL_TMP_VEL_CONTACT2A (LEAD_KEY);
--drop table DPL_TMP_VEL_CONTACT2A;
--create table DPL_TMP_VEL_CONTACT2A as
execute immediate 'truncate table DPL_TMP_VEL_CONTACT2A';
insert /* append nologging */ into DPL_TMP_VEL_CONTACT2A
select  /*+ parallel(b, 2) */
	LEAD_KEY,
	LEAD_KEY_SEQ,
	AGENT_EMAIL_SENT,
	LOG_DATE as FIRST_AGENT_EMAIL_SENT_DATE
from
	(	select /*+ parallel(a, 2) */
			LEAD_KEY,
			LEAD_KEY_SEQ,
			LOG_DATE,
			AGENT_EMAIL_SENT,
			rank() over (partition by LEAD_KEY order by LOG_DATE,rownum) as RANK
		from
			DPL_TMP_VEL_CONTACT1 a
		where
			AGENT_EMAIL_SENT='Y'
	) b
where RANK=1
order by 1,2;
commit;


--create index DPL_TMP_VEL_CONTACT2_X on DPL_TMP_VEL_CONTACT2 (lead_key);

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','12 - Get Attempt and Contact Info',Null,Null);

--drop table DPL_TMP_VEL_CONTACT3;
--create table DPL_TMP_VEL_CONTACT3 as
execute immediate 'truncate table DPL_TMP_VEL_CONTACT3';

insert /* append nologging */ into DPL_TMP_VEL_CONTACT3
select /*+ parallel(a, 2) parallel(b, 2) */
	LEAD_KEY,LEAD_KEY_SEQ,
	FIRST_CONTACT_ATTEMPT_DATE,FIRST_CONTACT_ATTEMPT_TYPE,TOTAL_CONTACT_ATTEMPTS,
	FIRST_CONTACT_MADE_DATE,FIRST_CONTACT_MADE_TYPE
from (
select /*+ parallel(a, 2) parallel(b, 2) */
	a.LEAD_KEY,
	a.lead_key_seq,
	a.LOG_DATE,
	rank() over (partition by a.LEAD_KEY order by a.LOG_DATE,rownum) as RANK,
	min(a.LOG_DATE) over (partition by a.LEAD_KEY order by a.LOG_DATE) as FIRST_CONTACT_ATTEMPT_DATE,
	greatest(1,SUM(a.NUM_ATTEMPTS) over (partition by a.LEAD_KEY)) as TOTAL_CONTACT_ATTEMPTS,
	first_value(a.LOG_TYPE_DESC) over (partition by a.LEAD_KEY order by a.LOG_DATE, rownum) FIRST_CONTACT_ATTEMPT_TYPE ,
	b.FIRST_CONTACT_MADE_DATE,
	b.FIRST_CONTACT_MADE_TYPE
from DPL_TMP_VEL_CONTACT1 a
	left join DPL_TMP_VEL_CONTACT2 b on a.LEAD_KEY=b.LEAD_KEY
where a.LOG_DATE<=nvl(b.FIRST_CONTACT_MADE_DATE,sysdate) and (NUM_ATTEMPTS=1 or LOG_TYPE_KEY=-99)
order by lead_key,log_date) a
where RANK=1;
--create index DPL_TMP_VEL_CONTACT3_X on DPL_TMP_VEL_CONTACT3 (LEAD_KEY);
commit;

SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','13 - MERGE Attempt and Contact Info into DPL_VELOCIFY_EXTRACT',Null,Null);

MERGE /*+ first_rows parallel(v, 2) parallel(c, 2) */
	INTO DPL_VELOCIFY_EXTRACT V
USING
	DPL_TMP_VEL_CONTACT3 c
ON (V.VELOCIFY_LEAD_ID = C.LEAD_KEY)
WHEN MATCHED THEN UPDATE SET
	V.FIRST_CONTACT_ATTEMPT_DATE 	= C.FIRST_CONTACT_ATTEMPT_DATE,
	V.TOTAL_CONTACT_ATTEMPTS 		= C.TOTAL_CONTACT_ATTEMPTS,
	V.FIRST_CONTACT_ATTEMPT_TYPE 	= C.FIRST_CONTACT_ATTEMPT_TYPE,
	V.FIRST_CONTACT_MADE_DATE 		= C.FIRST_CONTACT_MADE_DATE,
	V.FIRST_CONTACT_MADE_TYPE 		= C.FIRST_CONTACT_MADE_TYPE,
	V.CONTACT_MADE 					= case when C.FIRST_CONTACT_MADE_DATE is not null then 'Y' else 'N' end;
COMMIT;

MERGE /*+ first_rows parallel(v, 2) parallel(c, 2) */
	INTO DPL_VELOCIFY_EXTRACT V
USING
	DPL_TMP_VEL_CONTACT2A c
ON (V.VELOCIFY_LEAD_ID = C.LEAD_KEY and c.FIRST_AGENT_EMAIL_SENT_DATE<=nvl(v.FIRST_CONTACT_MADE_DATE,sysdate+10)) -- only include email sent if prior to FIRST_CONTACT_MADE_DATE
WHEN MATCHED THEN UPDATE SET
	V.AGENT_EMAIL_SENT 				= case when C.FIRST_AGENT_EMAIL_SENT_DATE is not null then 'Y' else 'N' end,
	V.FIRST_AGENT_EMAIL_SENT_DATE 	= C.FIRST_AGENT_EMAIL_SENT_DATE;

COMMIT;



execute immediate 'truncate table DPL_TMP_VEL_CONTACT1';
execute immediate 'truncate table DPL_TMP_VEL_CONTACT2';
execute immediate 'truncate table DPL_TMP_VEL_CONTACT2A';
execute immediate 'truncate table DPL_TMP_VEL_CONTACT3';
commit;

<<END_POINT>>
SP_LOG('MDM_LEAD_LOG','SP_DPL_01_VELOCIFY_EXTRACT','FINISH: SP_DPL_01_VELOCIFY_EXTRACT',null,null);

END SP_DPL_01_VELOCIFY_EXTRACT;

/
