--PL/SQL Script Part of CE LOANS process will require review (Ella) and then converted into a procedure
--Creates an OFFER by OFFER BPO table based on selection criteria in CAMPAIGN_EXCLUSIONS_T with offer type as BPO
TRUNCATE TABLE AUTO_CE_BPO;
COMMIT;

/
declare
                cursor c1 IS
                                select OFFER_KEY, SQL_STR
                                FROM CAMPAIGN_EXCLUSIONS_T
                                WHERE OFFER_KEY IN (SELECT DISTINCT OFFER_KEY FROM OFFER_T WHERE ACTIVE_FLAG = 'Y' AND METHOD_CODE IS NOT NULL)
                                AND TYPE = 'BPO';
begin

                FOR N in C1
                LOOP

      				execute immediate 'INSERT INTO AUTO_CE_BPO SELECT * FROM (SELECT ' || N.OFFER_KEY ||' AS OFFER_KEY,
      				A.ACCOUNT_NUMBER,
					A.TRANSACTION,
					A.NEW_PRODUCT,
					A.ENG_PROD_CD AS ECLIPSE_PROD_CD,
					A.NEW_TERM,
					A.NEW_LOAN_AMOUNT,
					A.LTV AS LTV_B,
					A.NEW_RATE,
					A.NEW_APR,
					A.NEW_POINTS_DOL,
					A.NEW_FEES_TOTAL AS NEW_FEES_DOL,
					A.NEW_POINTS_PCT,
					A.NEW_PI,
					A.NEW_PI_SAVINGS_MO,
					A.NEW_PIMI_SAVINGS_MO,
					A.NEW_CASHOUT,
					A.CRED_SCORE,
					A.NEW_TERM_REDUCTION AS TERM_REDUCTION,
					A.NEW_LOL_SAVINGS AS LOL_SAVINGS,
					A.CURRENT_PROPERTY_VALUE AS CPV_B,
					A.MARGIN AS ARM_MARGIN,
					0 AS ARM_LIBOR,
					A.LIFE_CAP AS ARM_LIFE_CAP,
					A.FIRST_ADJ AS ARM_FIRST_ADJ,
					A.FIRST_INC AS ARM_FIRST_INC,
					A.SEC_ADJ AS ARM_SEC_ADJ,
					A.SEC_INC AS ARM_SEC_INC,
					0 AS FULLY_INDEXED_RATE,
					0 AS ARM_PI_RESET,
					A.RATE_DT,
					A.RATE_NO,
					A.SUBCHANNEL_ID,
					A.CUR_MI_AMOUNT AS CURRENT_MI,
					A.NEW_MI,
					A.NEW_UFMIP, RANK() OVER (PARTITION BY ACCOUNT_NUMBER order by (CASE WHEN TRANSACTION LIKE (''%CASHOUT%'') THEN NEW_PI_SAVINGS_MO ELSE NEW_PIMI_SAVINGS_MO END) DESC, ROWNUM) AS RANK
					FROM BATCH_PRICING_OUTPUT A
					WHERE SERVICING_SOURCE <> ''MAN'' AND '
					|| N.SQL_STR || ') WHERE RANK = 1';
	COMMIT;
    END LOOP;
end;
/

COMMIT;