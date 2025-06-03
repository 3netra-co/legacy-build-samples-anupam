--PL/SQL Script Part of CE LOANS process will require review (Ella) and then converted into a procedure
--Reads all exclusions from CAMPAIGN_EXCLUSIONS_T and applies to CE_LOANS columns
/

DECLARE
vOFFER NUMBER;
vFIELD VARCHAR(100);
vMETHOD VARCHAR(20);
v_SCRIPT CLOB  :=NULL;
v_SCRIPT1 CLOB :=NULL;

--First COUNTER to fetch all offers from OFFER_T

CURSOR COUNTER1 IS
                                                                select offer_key, ce_loans_offer_field, ce_loans_excl_field, method_code from OFFER_T where active_flag = 'Y' and CE_LOANS_EXCL_FIELD is not null order by 1;

--Second COUNTER to fetch all exclusions from CAMPAIGN_EXCLUSIONS_T
CURSOR COUNTER2 IS
                                                                select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
                                                                from (select * from CAMPAIGN_EXCLUSIONS_T where offer_key = vOFFER and active_flag = 'Y' and TYPE <> 'BPO' order by exclusion_code);


BEGIN

--Offer Loop
                FOR OFFER in COUNTER1
                LOOP

                                vOFFER:=OFFER.OFFER_KEY;
                                vFIELD:=OFFER.CE_LOANS_EXCL_FIELD;
                                vMETHOD:=OFFER.METHOD_CODE;

--Exclusions Loop
                                                FOR EXCL in COUNTER2
                                                LOOP
                                                				v_SCRIPT1:= EXCL.EXCL_LIST;
                                                                v_SCRIPT := v_SCRIPT || ' ' || v_SCRIPT1;
                                                END LOOP;

                                execute immediate 'MERGE INTO CE_LOANS C USING  (SELECT A.ACCOUNT_NUMBER, CASE ' || v_SCRIPT ||
																'ELSE ''9999-SELECTED'' END AS EXCL
                												FROM AUTO_CE_DATAPULL A,
                												VW_CE_ANCILLARY VW_CE_ANCILLARY,
                												(SELECT * FROM AUTO_CE_BPO B WHERE OFFER_KEY = ' || vOFFER || ') B,
                												(SELECT ''' || vMETHOD || ''' AS METHOD_CODE FROM DUAL) C
                												WHERE A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER(+)
                												AND A.ACCOUNT_NUMBER = VW_CE_ANCILLARY.ACCOUNT_NUMBER(+)
                                                                ) W ON (C.ACCOUNT_NUMBER = W.ACCOUNT_NUMBER) WHEN MATCHED THEN UPDATE SET  C.'|| vFIELD ||' = W.EXCL';

                COMMIT;
                v_SCRIPT := NULL;
                END LOOP;

END;

/

COMMIT;
