OPTIONS (SKIP=1,ROWS=1000,BINDSIZE=20000000)
load data 
infile 'J:\GT-Marketing\Production Processes\Weekly\MDL - Models in Prod\PO Model Tranches\TRANCHE_TAB.csv' 
TRUNCATE
into table MDL_PO_ANALYSIS_BASE
fields terminated by ','
OPTIONALLY ENCLOSED BY '"' AND '"'
trailing nullcols
           ( 	COL_CD,
		VAL_CD,
		TAB,
		COL,
		COLUMN_TYPE,
		OPRT,
		VAL,
		GRP,
		ACTIVE_FLAG


           )
