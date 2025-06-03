--CAMPAIGN EXECUTION PROCESS PRE QC STEPS
--Runs for a single cell at a time (this revision was made post JB's modification of campaign plan
--SELECT * FROM CAMPAIGN_CELL_t WHERE TIME_PERIOD = '2014-05' AND CELL_DATE <= SYSDATE

--NOTES:  EMAIL files do not need suppression from previous files sent
--        Dialogue/OBTM files do not need supression from previous files sent - (IT updates dispo codes from vendor to manage)

--FOR SETUP


update campaign_cell_t
set offer_key = 9008,
time_period = to_char(trunc(sysdate),'yyyy-mm')
where cell_key = 11111;
commit;

update campaign_t
set offer_key = 9008
where campaign_key = 19999;
commit;

select * from campaign_cell_t where cell_key = 11111;


------------------------------------------
--UPDATE THE TABLE FOR CELL TO BE EXECUTED
------------------------------------------
CREATE TABLE CE_CELL (CELL_KEY NUMBER,  CELL_SIZE NUMBER, ALT_CELL_KEY NUMBER, SW_FLAG VARCHAR(1), OPTIONS_FLAG VARCHAR(1), CELL_SUPP VARCHAR(100));
INSERT INTO CE_CELL VALUES
(11111,			--CELL TO BE EXECUTED
100000000,        -- 1000000 is MAX QTY (DEFAULT) FOR THE CELL - IF REQUIRES SMALLER SIZE THEN PICK FROM CAMPAIGN PLAN
999999,			-- Use 999999 as ALT_CELL_KEY if you do not wish to retain remaining records. Use actual if want to split
'N',			--'Y' IF IT IS AN SW CAMPAIGN - PLEASE SEE NOTES BELOW
'Y',			--'Y' IF IT IS OPTIONS CAMPAIGN
'99999'    --    '99999'   --IF CELL REQUIRES SUPPRESSION FROM EXISTING CELLS IN SH OR SHT ENTERS CELL KEYS OR USE '99999'
);
COMMIT;

-- CE_SPL is a table that should contain account numbers qualifying for SW or Options 2nd offer

--CREATE FILE FOR OPTIONS MAIL
--  Criteria need to be revisited before running the code
CREATE TABLE CE_OPT_SCD AS
	SELECT *
	FROM
		(
		SELECT
		BP.*,
		RANK() OVER (PARTITION BY BP.ACCOUNT_NUMBER order by BP.NEW_TERM_REDUCTION desc, rownum) AS RANK
		FROM BATCH_PRICING_OUTPUT BP,
		MDM_LOAN LM
		WHERE BP.ACCOUNT_NUMBER = LM.ACCOUNT_NUMBER
		AND BP.SERVICING_SOURCE = LM.SERVICING_SOURCE
		AND BP.TRANSACTION = 'TERM REDUCER'
		AND NEW_PRODUCT LIKE ('%FIXED%')           -- 2-11-16 JE
		AND BP.ADJ_REMAINING_TERM >= 120
		AND BP.NEW_TERM_REDUCTION >= 24
		AND BP.NEW_TERM >= 120
		AND BP.NEW_LOL_SAVINGS >= 5000
		AND (LM.INTEREST_RATE *100) - BP.NEW_RATE >= .5
		)
	WHERE RANK = 1
	AND (SELECT OPTIONS_FLAG FROM CE_CELL) = 'Y';
COMMIT;

--CREATE FILE FOR SMARTWATCH MAIL
--  Criteria need to be revisited before running the code
START "\\ditech.us\data\GT-Marketing\Production Processes\Campaign Execution\1.0.1-SW_FLAT_FILE.sql";

--  This part of INSERT executes adding qualifying accounts for OPTIONS LETTER to CE_SPL which later are picked in the process
INSERT INTO CE_SPL (ACCOUNT_NUMBER)
SELECT
DISTINCT ACCOUNT_NUMBER
FROM CE_SW_OPTIONS
WHERE (SELECT SW_FLAG FROM CE_CELL) = 'Y';
COMMIT;


INSERT INTO CE_SPL (ACCOUNT_NUMBER)
SELECT
DISTINCT ACCOUNT_NUMBER
FROM CE_OPT_SCD
WHERE (SELECT OPTIONS_FLAG FROM CE_CELL) = 'Y';
COMMIT;


--Added by AT on 4/28 to allow for cash-out transaction where the control for cashout has to meet Coupon 9 X 11 (offer key 9008)
INSERT INTO CE_SPL (ACCOUNT_NUMBER)
SELECT
ACCOUNT_NUMBER
FROM BATCH_PRICING_OUTPUT
WHERE (
		(TRANSACTION = 'RATE TERM REFINANCE' AND (ADJ_REMAINING_TERM < 210 OR ADJ_REMAINING_TERM > 270))
		OR (TRANSACTION = 'TERM EXTENDER' AND ADJ_REMAINING_TERM >= 210 AND ADJ_REMAINING_TERM <= 270 AND NEW_TERM = 360)
      )
AND NEW_PIMI_SAVINGS_MO >= 70
AND NEW_PRODUCT NOT IN ('%FHA%')
AND LTV <= 80
AND (SELECT OFFER_KEY FROM CAMPAIGN_CELL_T WHERE CELL_KEY = (SELECT CELL_KEY FROM CE_CELL)) = 9013;
COMMIT;


--START PART 1-Setup campaign for a cell
START "\\ditech.us\data\GT-Marketing\Production Processes\Campaign Execution\1.1-CAMPAIGN_SETUP.sql";
COMMIT;

ALTER TABLE CE_DATAPULL
DROP (EXCL_CRITERIA, CELL_GRP1, CELL_GRP2);
COMMIT;

DROP TABLE CE_LOANS;
COMMIT;

CREATE TABLE CE_LOANS
AS SELECT A.*,
'P' AS BORROWER_TYPE
FROM CE_DATAPULL A;
COMMIT;

INSERT INTO CE_LOANS
SELECT L.*,
'S' AS BORROWER_TYPE
FROM CE_DATAPULL L
WHERE
	(
	 NVL(L.DNS_EMAIL_FLAG_SECD,'N') = 'N' --exclude secondary DNS
	 AND L.EMAIL_ADDRESS_SECD IS NOT NULL AND L.EMAIL_ADDRESS_SECD LIKE ('%@%') AND L.EMAIL_ADDRESS_SECD LIKE ('%.%') --exclude null secondary email
	 AND UPPER(NVL(L.EMAIL_ADDRESS_SECD,'XXXX')) <> UPPER(NVL(L.EMAIL_ADDRESS,'XXXX')) -- exclude where email address is same as PRIM
	 )
;
COMMIT;

create index CE_LOANS_NDX on CE_LOANS (ACCOUNT_NUMBER);
COMMIT;


DROP TABLE ce_cell;
DROP TABLE ce_opt_scd;

-- FROM 1.1-CAMPAIGN_SETUP
DROP TABLE BPO_SQL;
DROP TABLE CE_SOL_D;
DROP TABLE CE_SOL_R;
DROP TABLE CE_SOL_O;
DROP TABLE CE_RTL_CNT;
DROP TABLE CE_DATA_TEMP;
DROP TABLE CE_SOL_CNT;           -- Count of prior DM solicitations by ACCOUNT_NUMBER (Non Responder/Life Preserver group)

-- FROM 1.2-WATERFALL_REPORT
----DROP TABLE CE_EXCL_TEMP;
----DROP TABLE CE_EXCL;
--DROP TABLE CE_CELL1;
--DROP TABLE CE_EXCL;
--DROP TABLE CE_EXCL_TEMP;
--DROP TABLE SH_SUPP;
--
---- FROM 1.3-INSERT_RECORDS_SHT
--DROP TABLE CE_SAMPLE;
DROP TABLE CE_CAMPAIGN;
DROP TABLE CE_DATAPULL;
DROP TABLE CE_BPO;
DELETE FROM CE_SPL;
COMMIT;
