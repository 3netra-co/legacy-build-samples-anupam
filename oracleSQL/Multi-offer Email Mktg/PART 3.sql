
CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8001
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_1 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O1.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;


CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8002
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_2 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O2.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8003
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_3 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O3.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8004
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_4 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O4.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8005
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_5 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O5.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8006
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_6 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O6.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8007
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_7 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O7.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8008
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_8 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O8.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8009
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_9 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O9.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;


CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8010
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_10 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O10.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8011
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_11 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O11.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8012
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_12 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O12.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8013
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_13 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O13.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

--HAMP MOD
CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8006
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014,3008)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_14 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O14.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;  

--Forbearance loans
CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8006
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014,1005)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_15 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O15.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

--2nd lien DM
CREATE TABLE CE_EXCL_TEMP AS
select * from CAMPAIGN_EXCLUSIONS_T
where offer_key = 8108
and active_flag = 'Y'
and TYPE <> 'BPO' and EXCLUSION_CODE NOT IN (3021, 6013 , 6014, 1005)
order by exclusion_code;
COMMIT;


CREATE TABLE CE_EXCL (CTR NUMBER,  EXCL_LIST VARCHAR(3000));
COMMIT;


DECLARE
vCTR NUMBER;

CURSOR C1 IS
				select 'WHEN ' || SQL_STR || ' THEN ' || '''' || EXCLUSION_CODE || '-' || EXCLUSION_DESC || '''' AS EXCL_LIST
				from ce_excl_temp;


BEGIN
	vCTR:=0;
	INSERT INTO CE_EXCL VALUES (vCTR,'UPDATE CE_LOANS SET EXCL_16 = (CASE ');
	FOR N in C1
	LOOP
		vCTR:=vCTR+1;
		INSERT INTO CE_EXCL VALUES (vCTR,N.EXCL_LIST);
	END LOOP;

END;

/
INSERT INTO CE_EXCL VALUES (1000,'ELSE ''9999-SELECTED'' END) ;');

SELECT EXCL_LIST FROM CE_EXCL order by CTR;

SET NEWPAGE 0
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
spool \\ditech.us\data\gt-marketing\temp\O16.SQL
SELECT EXCL_LIST FROM CE_EXCL order by CTR;
spool off

DROP TABLE CE_EXCL;
DROP TABLE CE_EXCL_TEMP;
COMMIT;

update ce_loans
set method_code = 'EMAIL';
commit;

START "\\ditech.us\data\gt-marketing\temp\O1.SQL";
START "\\ditech.us\data\gt-marketing\temp\O2.SQL";
START "\\ditech.us\data\gt-marketing\temp\O3.SQL";
START "\\ditech.us\data\gt-marketing\temp\O4.SQL";
START "\\ditech.us\data\gt-marketing\temp\O5.SQL";

update ce_loans
set method_code = 'DM';
commit;

START "\\ditech.us\data\gt-marketing\temp\O6.SQL";
START "\\ditech.us\data\gt-marketing\temp\O7.SQL";
START "\\ditech.us\data\gt-marketing\temp\O8.SQL";
START "\\ditech.us\data\gt-marketing\temp\O9.SQL";

START "\\ditech.us\data\gt-marketing\temp\O14.SQL";
START "\\ditech.us\data\gt-marketing\temp\O15.SQL";
START "\\ditech.us\data\gt-marketing\temp\O16.SQL";

update ce_loans
set method_code = 'OTH';
commit;

START "\\ditech.us\data\gt-marketing\temp\O10.SQL";
START "\\ditech.us\data\gt-marketing\temp\O11.SQL";
START "\\ditech.us\data\gt-marketing\temp\O12.SQL";
START "\\ditech.us\data\gt-marketing\temp\O13.SQL";
COMMIT;
