stop;  -- avoid mistakenly running entire script

--------------------------------------------------------------------------------------------------------------------------------------------
-- MANUAL RUN:  Manually run the job
exec SP_DPA_03_APP_EXTRACT_SDR;

--------------------------------------------------------------------------------------------------------------------------------------------
-- View stored procedure compile errors
select * from all_errors where name='SP_DPA_03_APP_EXTRACT_SDR';
select * from all_source where name='SP_DPA_03_APP_EXTRACT_SDR';

--------------------------------------------------------------------------------------------------------------------------------------------
-- Find any apps missing on MDM_APP

--drop table DPA_MISSING_ENCOMPASS_APPS;
create table DPA_MISSING_ENCOMPASS_APPS as
select

	l.LOAN_SDR_KEY,
	'ENCOMPASS' app_extract_source,
	upper(l.encompass_id) encompass_id,
	l.loan_number app_no,

	l.Loan_Folder APP_LOAN_FOLDER,
	l.Adverse_Action_Date,
	l.action_taken,
	cd2.cx_today


from
	SDR_CORE.EMLOS_LOANS_HRLY_BASE@renpup2sdrp l
	left join MDM_APP a on l.loan_number=a.APP_no
	left join SDR_CORE.EMLOS_CUSTOM2_HRLY_DATES@renpup2sdrp cd2
			on L.LOAN_SDR_KEY = CD2.LOAN_SDR_KEY
where l.Loan_Folder not in ('(Trash)','Testing_Training') and a.app_no is null;

select * from  DPA_MISSING_ENCOMPASS_APPS;

select count(*) from DPA_ENCOMPASS_EXTRACT_SDR;

select count(*) from DPA_APP_ADD_UPDATE;

/

--------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE STORED PROCEDURE:

CREATE OR REPLACE PROCEDURE SP_DPA_03_APP_EXTRACT_SDR AS

date_now date :=trunc(SYSDATE);
vRecordCnt integer;

BEGIN


--execute immediate 'ALTER SESSION ENABLE PARALLEL DML';

SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','START: SP_DPA_03_APP_EXTRACT_SDR',null,null);


-- Step 1:   Incremental extract of Encompass data from the SDR into DPA_ENCOMPASS_EXTRACT_SDR.

SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','1 - INSERT into DPA_ENCOMPASS_EXTRACT_SDR from SDR Encompass tables',null,null);


EXECUTE IMMEDIATE 'TRUNCATE table DPA_ENCOMPASS_EXTRACT_SDR';

INSERT /*+ append */ INTO DPA_ENCOMPASS_EXTRACT_SDR nologging
(	LOAN_SDR_KEY,
	APP_EXTRACT_SOURCE,
	ENCOMPASS_ID,
	APP_NO,
	APP_PRIOR_LOAN_NO,
	APP_SAME_SERVICER_FLAG,
	APP_ENTITY_CD,
	APP_CHAN_CD,
	APP_CHAN_GROUP,
	APP_BRANCH,
	APP_LNOFF_CD,
	APP_PRODUCT_CODE,
	APP_CREATE_DATE,
	APP_REG_A_DATE,
	APP_SETUP_DATE,
	APP_FUND_DATE,
	APP_CANCEL_DATE,
	APP_STATUS_CODE,
	APP_STATUS_GRP,
	APP_STATUS_DATE,
	ORIGINAL_APP_SOURCE_CD,
	APP_PURPOSE_CODE,
	APP_LOAN_AMT,
	APP_TERM,
	APP_RATE,
	APP_DISC_PTS,
	APP_PI,
	APP_PI_CALCD,
	APP_P_STREET_ADDRESS_1,
	APP_P_STREET_ADDRESS_2,
	APP_P_CITY,
	APP_P_STATE,
	APP_P_ZIP,
	APP_APPRAISED_VALUE,
	APP_ESTD_LTV,
	APP_APPRAISAL_LTV,
	APP_PROCESSING_STYLE,
	APP_CANCEL_REASON,
	APP_HMDA_ACTION,
	APP_HMDA_DESC,
	APP_DECLINE_CD,
	APP_PROD_CD,
	APP_PROC_TYP,
	DATE_ADDED,
	APP_LO_NAME,
	APP_OCCUPANCY,
	APP_NUM_UNITS,
	APP_DECL_DTI,
	APP_DECL_EMPL_HIST,
	APP_DECL_CREDIT_HIST,
	APP_DECL_COLLATERAL,
	APP_DECL_INS_CASH,
	APP_DECL_UNVER_INFO,
	APP_DECL_APP_INCOMP,
	APP_DECL_MI_DENIED,
	APP_DECL_OTHER,
	APP_PRODUCT_NAME,
	APP_PROC_TEAM,
	APP_PROC_CD,
	CRED_SCORE,
	APP_ARM_TYPE,
	AM_TYP,
	TIER_CD,
	REFI_CD,
	SUBFI_AMT,
	BLD_STYLE,
	MORT_TYP,
	FNMA_CRED_SCORE_YN,
	TYPE_CD,
	PROP_TYP,
	AFFINITY_RELTN_CD,
	TAX_WAIVE,
	PROG_TYP,
	LLPA_LTV,
	LLPA_CLTV,
	APP_RATE_LOCK_DATE,
	APP_RATE_LOCK_DATE_FIRST,
	APP_PROD_GROUP,
	SERVICER_LOAN_NO,
	SERVICER_NAME,
	LOAN_SOURCE,
	APP_SOURCE,
	RATE_LOCK_CODE,
	RATE_LOCK_TYPE,
	BRAND,
	APP_CURR_MILESTONE_NAME,
	APP_APR,
	FIRST_PAYMENT_DATE,
	FIRST_RATE_ADJ_DATE,
	APP_PROP_FIRST_MORT_AMT,
	BALLOON_PMT_DUE_YEARS,
	APP_RLA_RETURNED_DATE,
	APP_RL_TIMES_EXTENDED,
	APP_LOAN_FOLDER,
	APP_AGENT_EMAIL,
	APP_AGENT_WORK_PHONE,
	APP_AGENT_NMLS_ID,
	APP_MILITARY_BORR_IND,
	APP_MILITARY_COBORR_IND,
	APP_MILITARY_DOC_VER_DATE,
	APP_PREAPPROVAL,
	APP_PREQUAL,
	APP_COND_APPROV_DATE,
	VELOCIFY_LEAD_ID,
	APP_MSR_OWNER,
	APP_M_STREET_ADDRESS_1,
	APP_M_CITY,
	APP_M_STATE,
	APP_M_ZIP,
	CX_PP_TEAM,
	LOAN_PURPOSE_TYPE,
	CX_LOAN_CHANNEL_CD,
	APP_SUBORD_FLAG,
	APP_SUBORD_AMT,
	APP_MONTHLY_MI,
  BLEND_ID)



select

/*+ DRIVING_SITE(l)
	parallel(l,2)
	parallel(p,2)
	parallel(m,2) parallel(r1,2) parallel(c3,2) parallel(c4,2)
	parallel(c5,2) parallel(c6,2) parallel(c7,2) parallel(c9,2)
	parallel(cd1,2) parallel(cd2,2) parallel(res,2)
*/
--parallel(b1,2) parallel(b2,2)

	l.LOAN_SDR_KEY,
	'ENCOMPASS' app_extract_source,
	upper(l.encompass_id) encompass_id,
	l.loan_number app_no,
	c5.CX_SSR_LOAN_NUMBER app_prior_loan_no,
	c5.CX_SSR_INDICATOR  app_same_servicer_flag,
	'NEWCO' app_entity_cd,
	case
		WHEN c4.cx_pp_team LIKE 'RETAIL%' then 'Retail'
		when c4.CX_LOAN_CHANNEL_CD = 'AQST'
			then 'Consumer Direct'
		when c4.CX_LOAN_CHANNEL_CD = 'RETL'
			then 'Retention'
		when c4.CX_LOAN_CHANNEL_CD = 'RET2'
			then 'Retail'
		else 'OTHER'
	end app_chan_cd,
	case when c4.cx_loan_channel_cd = 'RETL' then 'Retention' else 'Acquisition' end app_chan_group,
	l.organization_code app_branch,
	REGEXP_REPLACE (UPPER (l.interviewers_id), '(.*-)') app_lnoff_cd,
	rl.plan_code app_product_code,
	trunc(l.MILESTONE_FILE_STARTED_DATE) app_create_date,
	trunc(M.REG_DATE) app_reg_a_date,
	case
		when trunc(ms_setup_first_date) is null then trunc(ms_pre_uw_first_date)
		else trunc(ms_setup_first_date)
	end  app_setup_date,
	trunc(M.MS_POST_CLOSE_first_DATE) app_fund_date,
	case
		when l.Action_Taken in
				('Application withdrawn',
				'Application denied',
				'Application approved but not accepted',
				'File Closed for incompleteness',
				'Preapproval request denied by financial institution',
				'Preapproval request approved but not accepted')
			then l.Adverse_Action_Date
		else NULL
	end app_cancel_date,
	case
		when l.action_taken in
				('Application withdrawn',
				'Application denied',
				'Application approved but not accepted',
				'File Closed for incompleteness',
				'Preapproval request denied by financial institution',
				'Preapproval request approved but not accepted')
			then 'Cancel'
		else l.MILESTONE_CURRENT_NAME
	end app_status_code,
	case
		when l.action_taken in
				('Application withdrawn',
				'Application denied',
				'Application approved but not accepted',
				'File Closed for incompleteness',
				'Preapproval request denied by financial institution',
				'Preapproval request approved but not accepted')
			then 'Cancelled'
		when l.action_taken = 'Loan Originated'
			then 'Funded'
		-- Stone updated logic on 1/17/2018 to consider all loans not 'ended' and with Setup Date to be PIPELINE
		when l.action_taken IS NULL and nvl(ms_setup_first_date,ms_pre_uw_first_date) is not null
			then 'Pipeline'
		when l.action_taken IS NULL and nvl(ms_setup_first_date,ms_pre_uw_first_date) is null
			then 'Lead'
		else '***UNKNOWN:'||l.action_taken
	end  app_status_grp,
	case
		when l.MILESTONE_CURRENT_DATE_UTC = to_date('11/12/2013','mm/dd/yyyy')
			then
				case
					when l.Action_Taken in
						('Application withdrawn',
						'Application denied',
						'Application approved but not accepted',
						'File Closed for incompleteness',
						'Preapproval request denied by financial institution',
						'Preapproval request approved but not accepted')
						then l.Adverse_Action_Date
					when m.reg_date is not null then m.reg_date
					else l.MILESTONE_FILE_STARTED_DATE
					end
		else l.MILESTONE_CURRENT_DATE_UTC
	end app_status_date,
	l.loan_source as original_app_source_cd,
	case
		when p.Loan_Purpose_Type like '%Refinance%'then 'R'
		when p.Loan_Purpose_Type like '%Purchase%' then 'P'
		when p.Loan_Purpose_Type like '%Construction%' then 'P'
		when p.Loan_Purpose_Type = 'Other' then 'O'
		else '*******('||p.Loan_Purpose_Type||')'
	end app_purpose_code,
	to_number(l.base_loan_amt/*l.BORROWER_REQUESTED_LOAN_AMT*/) app_loan_amt,
	to_number(l.LOAN_AMORTIZATION_TERM_MONTHS) app_term,
	to_number(l.REQ_INTEREST_RATE_PERCENT) app_rate,
	to_number(c3.CX_FINALPRICETOTALPOINTS) app_disc_pts,
	to_number(l.PI_MONTHLY_PAYMENT) app_pi,
	case
	    when l.REQ_INTEREST_RATE_PERCENT = 0 or l.LOAN_AMORTIZATION_TERM_MONTHS = 0 then 0
	    else round (((l.REQ_INTEREST_RATE_PERCENT / 1200)/(1-power(1+l.REQ_INTEREST_RATE_PERCENT/1200,-l.LOAN_AMORTIZATION_TERM_MONTHS)))*l.BORROWER_REQUESTED_LOAN_AMT,2)
	end app_pi_calcd,
	upper(p.street_Address) app_p_street_address_1,
	' ' app_p_street_address_2, -- always null from Encompass
	upper(p.city) app_p_city,
	upper(p.state) app_p_state,
	p.postal_code app_p_zip,
	to_number(l.PROPERTY_APPRAISED_VALUE_AMT) app_appraised_value,
	round(
		case
			when l.PURCHASE_PRICE_AMT > 0
			then l.BORROWER_REQUESTED_LOAN_AMT / l.purchase_price_amt
			else 0
		end *100,3) app_estd_ltv,
	round(
		case
			when l.PROPERTY_APPRAISED_VALUE_AMT > 0
			then l.BORROWER_REQUESTED_LOAN_AMT / l.PROPERTY_APPRAISED_VALUE_AMT
			else 0
		end *100,3) app_appraisal_ltv,
	nvl(LOAN_PURPOSE_OF_REFINANCE_TYPE,processing_style) app_processing_style, -- always null in Encompass
	case
                when L.Action_Taken in
                                ('Application denied',
                                'Preapproval request denied by financial institution')
                                then 'Declined'
                when L.Action_Taken in
                                ('Application withdrawn',
                                 'Application approved but not accepted',
                                 'File Closed for incompleteness',
								'Preapproval request approved but not accepted')
                                then 'Withdrawn'
                else null
	end app_cancel_reason,
	' ' app_hmda_action, -- always null in Encompass
	L.ACTION_TAKEN app_hmda_desc,
	' ' app_decline_cd, -- always null in Encompass
	l.mortgage_type || ' - ' || l.conforming_jumbo app_prod_cd,
	' ' app_proc_typ,  -- always null in Encompass
	date_now as  date_added,
	l.LOAN_OFFICER_NAME app_lo_name,
	substr(p.PROPERTY_USAGE_TYPE,1,1) app_occupancy,
	p.FINANCED_NUMBER_OF_UNITS app_num_units,
	case
		when l.decline_Reason1 = 'Debt to Income Ratio'
			or l.decline_Reason2 = 'Debt to Income Ratio'
			or l.decline_Reason3 = 'Debt to Income Ratio'
		then 1
	else 0 end as APP_DECL_DTI,

	case
		when l.decline_Reason1 = 'Employment history'
			or l.decline_Reason2 = 'Employment history'
			or l.decline_Reason3 = 'Employment history'
		then 1
	else 0 end as APP_DECL_EMPL_HIST,

	case
		when l.decline_Reason1 = 'Credit history'
			or l.decline_Reason2 = 'Credit history'
			or l.decline_Reason3 = 'Credit history'
		then 1
	else 0 end as APP_DECL_CREDIT_HIST,

	case
		when l.decline_Reason1 = 'Collateral'
			or l.decline_Reason2 = 'Collateral'
			or l.decline_Reason3 = 'Collateral'
		then 1
	else 0 end as APP_DECL_COLLATERAL,

	case
		when l.decline_Reason1 = 'Insufficient Cash'
			or l.decline_Reason2 = 'Insufficient Cash'
			or l.decline_Reason3 = 'Insufficient Cash'
		then 1
	else 0 end as APP_DECL_INS_CASH,

	case
		when l.decline_Reason1 = 'Unverifiable Information'
			or l.decline_Reason2 = 'Unverifiable Information'
			or l.decline_Reason3 = 'Unverifiable Information'
		then 1
	else 0 end as APP_DECL_UNVER_INFO,

	case
		when l.decline_Reason1 = 'Credit application Incomplete'
			or l.decline_Reason2 = 'Credit application Incomplete'
			or l.decline_Reason3 = 'Credit application Incomplete'
		then 1
	else 0 end as APP_DECL_APP_INCOMP,

	case
		when l.decline_Reason1 = 'Mortgage insurance denied'
			or l.decline_Reason2 = 'Mortgage insurance denied'
			or l.decline_Reason3 = 'Mortgage insurance denied'
		then 1
	else 0 end as APP_DECL_MI_DENIED,

	case
		when l.decline_Reason1 = 'Other'
			or l.decline_Reason2 = 'Other'
			or l.decline_Reason3 = 'Other'
		then 1
	else 0 end as APP_DECL_OTHER,
	l.loan_program_name app_product_name,
	' ' app_proc_team, -- always null in Encompass
	' ' app_proc_cd, -- always null in Encompass
	to_number(l.credit_Score_to_use) cred_score,
	' ' app_arm_type, -- always null in Encompass
	L.Loan_Amortization_Type AM_TYP,
	' ' TIER_CD,-- always null in Encompass
	' '  REFI_CD,-- always null in Encompass
	' ' SUBFI_AMT,-- always null in Encompass
	' ' BLD_STYLE,-- always null in Encompass
	L.Mortgage_Type MORT_TYP,
	' ' FNMA_CRED_SCORE_YN,-- always null in Encompass
	' ' TYPE_CD,-- always null in Encompass
	' ' PROP_TYP,-- always null in Encompass
	' ' AFFINITY_RELTN_CD,-- always null in Encompass
	' ' TAX_WAIVE,-- always null in Encompass
	' ' PROG_TYP,    -- always null in Encompass
	' ' LLPA_LTV,-- always null in Encompass
	' ' LLPA_CLTV ,-- always null in Encompass
	TRUNC(RL.LOCK_DATE) app_rate_lock_date,
	TRUNC(cd1.CX_LOCKDATETIME)  app_rate_lock_date_first,
	cast(null as VARCHAR2(20) ) as app_prod_group,

	case
		when m.ms_post_close_first_date is not null	then
			case
				when c5.CX_SERVICER_NAME = 'GreenTree' then c5.CX_SERVICING_LOAN_NO||c5.cx_servicing_checkdigit
				when c5.CX_SERVICER_NAME = 'GreenTreeMSP' then lpad(c5.CX_SERVICING_LOAN_NO||c5.cx_servicing_checkdigit,10,'0')
			else c5.CX_SERVICING_LOAN_NO end
		else null
	end FUNDED_APP_SVC_ACCT_NO,
	case
		when m.ms_post_close_first_date is not null
			then c5.CX_SERVICER_NAME
		else null
	end servicer_name,
	l.loan_source,
	case
		when instr(l.loan_source,'[') > 0
		then substr(l.loan_source,instr(l.loan_source,'[')+1,5)
		else null
	end app_source,
	c3.CX_INITIALLOCKWINDOW APP_RATE_LOCK_CODE,
	case
		when c4.CX_LOCKDATETIME is null then 'FLOATING'
		when (to_number(c3.cx_initiallockwindow) + nvl(cd1.CX_LOCKDATETIME, rl.lock_date)) > date_now
		AND l.action_taken in
				('Application withdrawn',
				'Application denied',
				'Application approved but not accepted',
				'File Closed for incompleteness',
				'Preapproval request denied by financial institution',
				'Preapproval request approved but not accepted',
				'Loan Originated')
				THEN 'EXPIRED'
			ELSE 'LOCKED'
	END  APP_RATE_LOCK_TYPE,
	CASE WHEN c4.CX_LOAN_CHANNEL_CD = 'RETL' THEN 'GREENTREE' ELSE 'DITECH' END BRAND,
	L.MILESTONE_CURRENT_NAME APP_CURR_MILESTONE_NAME,
	to_number(l.apr) APP_APR,
    l.FIRST_PAYMENT_DATE FIRST_PAYMENT_DATE,
	l.FIRST_PAYMENT_ADJ_DATE FIRST_RATE_ADJ_DATE,
	to_number(l.PROPOSED_FIRST_MORTGAGE_AMT) APP_PROP_FIRST_MORT_AMT,
	l.balloon_payment_due_in_years AS balloon_pmt_due_years,
	cd1.cx_lockagreementreceived app_rla_returned_date,
	cast(null as integer) APP_RL_TIMES_EXTENDED,
	l.Loan_Folder APP_LOAN_FOLDER,
	l.loan_officer_email APP_AGENT_EMAIL,
	l.loan_officer_phone APP_AGENT_WORK_PHONE,
	l.NMLS_LOANORIGINATOR_ID APP_AGENT_NMLS_ID,
	c6.CX_MILITARY_DISC_BORR_IND app_military_borr_ind,
	c7.CX_MILITARY_DISC_COBORR_IND app_military_coborr_ind,
	cd2.cx_military_docverified_date app_military_doc_ver_date,
	c4.cx_preapproval app_preapproval,
	c4.cx_prequal app_prequal,
	nvl(TRUNC(l.UW_CREDIT_APPROVED_DATE), trunc(l.UW_APPROV_DATE)) APP_COND_APPROV_DATE,
	case -- Because Velocify ID will be put into a numeric field on MDM_APP, must avoid any values that are not null or numeric.  On 8/7/2017, 'pending' was passed and caused errors later.
		when length(trim(translate(c3.cx_leadmanager_id,'1234567890 ','           ')) )>0 then null
		else c3.cx_leadmanager_id
	end as velocify_lead_id ,
	c9.CX_MSR_OWNER APP_MSR_OWNER,
	upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(res.address_street_line1)) app_m_street_address_1,
	upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(res.address_city)) app_m_city,
	upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(res.address_state)) app_m_state,
	upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(res.address_postal_code)) app_m_zip,

	c4.cx_pp_team,
	P.Loan_Purpose_Type,
	C4.Cx_Loan_Channel_Cd,
  Case When To_Number(L.Combinedltv) > To_Number(l.Ltv) Then 'Y' Else 'N' End App_Subord_Flag,
  case when To_Number(L.Combinedltv) > To_Number(l.Ltv) then  to_number(l.second_subordinate_amt) else null end  app_subord_amt,
  nvl(to_number(MI_PRE_DUE),0) as APP_MONTHLY_MI,
  c9.CX_BLEND_ID AS BLEND_ID


from
	SDR_CORE.EMLOS_LOANS_HRLY_BASE@renpup2sdrp l
		left join SDR_CORE.EMLOS_LOANS_HRLY_PROPERTY@renpup2sdrp p
			on l.LOAN_SDR_KEY = p.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_MILESTONES_HRLY_DATES@renpup2sdrp m
			on l.loan_sdr_key = m.loan_sdr_key
		left join SDR_CORE.EMLOS_RATE_HRLY_LOCK@renpup2sdrp rl
			on l.LOAN_SDR_KEY = rl.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM3_HRLY_FIELDS@renpup2sdrp c3
			on l.LOAN_SDR_KEY = c3.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM4_HRLY_FIELDS@renpup2sdrp c4
			on l.LOAN_SDR_KEY = c4.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM5_HRLY_FIELDS@renpup2sdrp c5
			on l.LOAN_SDR_KEY = c5.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM6_HRLY_FIELDS@renpup2sdrp c6
			on l.LOAN_SDR_KEY = c6.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM7_HRLY_FIELDS@renpup2sdrp c7
			on l.LOAN_SDR_KEY = c7.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM9_HRLY_FIELDS@renpup2sdrp c9
			on l.LOAN_SDR_KEY = c9.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM1_HRLY_DATES@renpup2sdrp cd1
			on L.LOAN_SDR_KEY = CD1.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_CUSTOM2_HRLY_DATES@renpup2sdrp cd2
			on L.LOAN_SDR_KEY = CD2.LOAN_SDR_KEY
		left join SDR_CORE.EMLOS_BORROWER_HRLY_RESIDENCE@renpup2sdrp res
			on l.loan_sdr_key = res.loan_sdr_key
			and upper(res.applicant_type) = 'BORROWER'
			and residence_version = 1
			and res.borrower_no = 1
			and residency_type = 'MAILING'
--
--
--
--where rownum < 10
where

	l.loan_created_date_utc >= TO_DATE('1/1/2014','MM/DD/YYYY')
	And Upper(L.Encompass_Id) <> 'EE5AF429-77A2-4DB5-9192-782F3D65C6C1'

	-- No need to keep re-pulling accounts that cancelled more than 60 days ago
	and NOT ( 		nvl(l.Adverse_Action_Date,date_now)<date_now-60
				and l.action_taken in
					(	'Application withdrawn',
						'Application denied',
						'Application approved but not accepted',
						'File Closed for incompleteness',
						'Preapproval request denied by financial institution',
						'Preapproval request approved but not accepted'    )
					)
	-- No need to keep re-pulling accounts that funded more than 60 days ago
	and NOT ( nvl(M.MS_POST_CLOSE_first_DATE,date_now)<date_now-60 and l.action_taken = 'Loan Originated')
	and not cd2.cx_today<date_now-60

	--l.loan_number in (select app_no from DPA_MISSING_ENCOMPASS_APPS)

-- use a variation of the WHERE condition below if you need to join to MDM_APP to fix issues. Stone, May 2017
--and (
--				ma.APP_M_STREET_ADDRESS_1 is null
--				or
--				nvl(upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(res.address_city)),'NULL') <> nvl(upper(ma.app_m_city),'NULL' )
--				 )
;

commit;

select COUNT(*) into vRecordCnt from ( select APP_NO from DPA_ENCOMPASS_EXTRACT_SDR b group by APP_NO having COUNT(*)>1 );
SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','1a - Delete any apps with dup APP_NO', vRecordCnt, null);

-- Delete any APP_NO records with dups
delete from DPA_ENCOMPASS_EXTRACT_SDR a
where app_no in
(select APP_NO from DPA_ENCOMPASS_EXTRACT_SDR b group by APP_NO having COUNT(*)>1);






----------------------------------------------------------------------------------------------------
-- Step 2:   Get borrower info

SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','2a - TRUNC/INSERT DPA_BORROWER_INFO',null,null);


execute immediate 'TRUNCATE TABLE DPA_BORROWER_INFO';
INSERT /*+ APPEND */
INTO DPA_BORROWER_INFO nologging
(	LOAN_SDR_KEY,
	APPLICANT_TYPE,
	APPLICATION_INDEX,
	BORROWER_FIRST,
	BORROWER_LAST,
	BORROWER_SSN,
	BORROWER_HOME_PHONE,
	BORROWER_EMAIL,
	BORROWER_FICO,
	BOROWER_BIRTH_DATE,
	BORROWER_RANK )

  Select *
from
	(	select /*+ DRIVING_SITE(b) 	parallel(b,2) */
			b.LOAN_SDR_KEY,
			b.applicant_type,
			b.application_index,
			upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(b.first_name)) borrower_first,
			upper(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(b.last_name)) borrower_last,
					replace(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(b.TAX_IDENTIFICATION_ID),'-','') borrower_ssn,
					substr(replace(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(b.HOME_PHONE_NUMBER),'-',''),1,10) borrower_home_phone,
					lower(SDR_DELTA.PKG_CRYPTO.decrypt@renpup2sdrp(b.email_address_Text)) borrower_email,
			b.middle_fico_score borrower_fico,
			b.birth_date as borower_birth_date,
			rank () over (partition by b.LOAN_SDR_KEY order by b.applicant_type, b.application_index) borrower_rank
		from SDR_CORE.EMLOS_LOANS_HRLY_BORROWER@renpup2sdrp B
       left join SDR_CORE.EMLOS_LOANS_HRLY_BASE@renpup2sdrp l
          on l.LOAN_SDR_KEY = b.LOAN_SDR_KEY
       left join SDR_CORE.EMLOS_MILESTONES_HRLY_DATES@renpup2sdrp m
        	on b.loan_sdr_key = m.loan_sdr_key
      left join SDR_CORE.EMLOS_CUSTOM2_HRLY_DATES@renpup2sdrp cd2
        	on b.LOAN_SDR_KEY = CD2.LOAN_SDR_KEY
	where

	l.loan_created_date_utc >= TO_DATE('1/1/2014','MM/DD/YYYY')
	And Upper(L.Encompass_Id) <> 'EE5AF429-77A2-4DB5-9192-782F3D65C6C1'

	-- No need to keep re-pulling accounts that cancelled more than 60 days ago
	and NOT ( 		nvl(l.Adverse_Action_Date,date_now)<date_now-60
				and l.action_taken in
					(	'Application withdrawn',
						'Application denied',
						'Application approved but not accepted',
						'File Closed for incompleteness',
						'Preapproval request denied by financial institution',
						'Preapproval request approved but not accepted'    )
					)
	-- No need to keep re-pulling accounts that funded more than 60 days ago
	and NOT ( nvl(M.MS_POST_CLOSE_first_DATE,date_now)<date_now-60 and l.action_taken = 'Loan Originated')
	and not cd2.cx_today<date_now-60
  )
WHERE borrower_rank<=2;
commit;


UPDATE DPA_BORROWER_INFO set
borrower_ssn = LM_CLEAN.LM_VALIDATE_SSN(borrower_ssn),
borrower_home_phone=LM_CLEAN.LM_VALIDATE_PHONE(borrower_home_phone),
borrower_email=LM_CLEAN.LM_VALIDATE_EMAIL(borrower_email)
;
commit;


SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','2b - MERGE to decrypt borrower info',null,null);

merge into DPA_ENCOMPASS_EXTRACT_SDR a
using DPA_BORROWER_INFO b
on (a.LOAN_SDR_KEY=b.LOAN_SDR_KEY and b.borrower_rank=1)
when matched then update
set
	a.app_prim_first 		= b.borrower_first,
	a.app_prim_last 		= b.borrower_last,
	a.app_prim_ssn 			= b.borrower_ssn,
	a.app_prim_ssn4 		= substr(b.borrower_ssn,-4),
	a.app_prim_home_phone 	= b.borrower_home_phone,
	a.app_prim_email 		= b.borrower_email,
	a.app_prim_fico 		= b.borrower_fico,
	a.app_prim_birth_date	= b.borower_birth_date;


SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','3c - MERGE to decrypt coborrower info',null,null);

merge into DPA_ENCOMPASS_EXTRACT_SDR a
using DPA_BORROWER_INFO b
on (a.LOAN_SDR_KEY=b.LOAN_SDR_KEY and b.borrower_rank=2)
when matched then update
set
	a.app_secd_first = b.borrower_first,
	a.app_secd_last = b.borrower_last,
	a.app_secd_ssn = b.borrower_ssn,
	a.app_secd_ssn4 = b.borrower_ssn4,
	a.app_secd_fico = b.borrower_fico;






----------------------------------------------------------------------------------------------------
-- Step 3:   Assign product group values

SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','3 - MERGE to update APP_PROD_GROUP',null,null);

MERGE INTO DPA_ENCOMPASS_EXTRACT_SDR X
USING
(select distinct app_no,
	CASE
		when APP_PRODUCT_NAME like '%FarmersHomeAdministration%' or  APP_PROD_CD like '%FarmersHomeAdministration%'
      then 'FHA OTHER'
		when pl.harp_flag = 'Y' and upper(pl.product_description) like '%DU%'
      then 'HARP DURP'
		when pl.harp_flag = 'Y'
      then 'HARP MANUAL'
		when pl.product_group in ('Conforming','High Balance')
      then 'CONVENTIONAL'
		when (pl.product_group in ('FHA','FHA High Balance') or ma.app_prod_CD like 'FHA%') AND APP_PROCESSING_STYLE LIKE '%Streamline%'
      then 'FHA STREAMLINE'
		when (pl.product_group in ('FHA','FHA High Balance') or ma.app_prod_CD like 'FHA%')
      then 'FHA OTHER'
		when (pl.product_group = 'VA' or ma.app_prod_CD like 'VA%') and app_processing_style in ('StreamlineWithoutAppraisal','InterestRateReductionRefinanceLoan')
      then 'VA IRRRL'
		when (pl.product_group = 'VA' or ma.app_prod_CD like 'VA%')
      then 'VA OTHER'
		when pl.product_group = 'USDA' or app_product_name='USDA Fixed'
      then 'USDA'
    when app_product_name = 'Conventional Manual HARP'
      then 'HARP MANUAL'
    when app_product_name = 'Conventional Fixed'
      then 'CONVENTIONAL'
    when app_product_name like '%FHA%'
      THEN 'FHA OTHER'
    WHEN APP_PRODUCT_NAME LIKE '%VA%'
      THEN 'VA OTHER'
    WHEN APP_PRODUCT_NAME LIKE '%Jumbo%'
      THEN 'JUMBO'
    WHEN APP_PRODUCT_NAME like '%Manual HARP%'
      then 'HARP MANUAL'
    WHEN APP_PRODUCT_NAME like 'Refi Plus Manual%'
      then 'HARP MANUAL'
    WHEN APP_PRODUCT_NAME IS NULL AND ma.APP_PROD_GROUP IN ('FHA','VA')
      THEN APP_PROD_GROUP || ' OTHER'
    WHEN APP_PRODUCT_NAME IS NULL AND ma.APP_PROD_GROUP IS NOT NULL
      THEN 'CONVENTIONAL'
		when APP_PROD_CD ='Conventional - Conforming'
      then 'CONVENTIONAL'
		when APP_PROD_CD ='Conventional - Jumbo'
      then 'JUMBO'
		when APP_PROD_CD ='FHA - Conforming'
      then 'FHA OTHER'
		when pl.product_group is not null
      then upper(pl.product_group)
		else 'UNKNOWN'
	END as APP_PROD_GROUP
from
    DPA_APP_EXTRACT_T ma
		left join mdm_product_list pl
      on ma.app_product_code = pl.product_code
) b
on (x.app_no=b.app_no)
when matched then update
set x.APP_PROD_GROUP=b.APP_PROD_GROUP;

commit;



SP_LOG('MDM_APP_LOG','SP_DPA_03_APP_EXTRACT_SDR','END: SP_DPA_03_APP_EXTRACT_SDR',null,null);



END SP_DPA_03_APP_EXTRACT_SDR;

/
