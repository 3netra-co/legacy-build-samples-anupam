Exec SP_LOG('MDM_CAMP_PROC_LOG', '*********', '********************  CE_LOANS START  ************************');
Exec SP_LOG('MDM_CAMP_PROC_LOG', ' ', ' ');
commit;

--To run process for CE_LOANS and CAMPAIGN_WATERFALL_SUMMARY
START "\\ditech.us\data\GT-Marketing\Production Processes\Campaign Execution\Campaign Waterfalls\01-BUILD CE LOANS (All Offers With BPO).sql";
commit;

Exec SP_LOG('MDM_CAMP_PROC_LOG', '*********', '********************  CE_LOANS DONE  *************************');
---Exec SP_LOG('MDM_CAMP_PROC_LOG', '*********', '***********************************************');
Exec SP_LOG('MDM_CAMP_PROC_LOG', ' ', ' ');
commit;
/*
--To run process for BPO tran scoring and new DM reengineering counts
START "\\ditech.us\data\GT-Marketing\Production Processes\Weekly\MDL - Models in Prod\Transaction Scoring Top 2 Trans.sql";
commit;
START "\\ditech.us\data\GT-Marketing\Production Processes\Weekly\MDL - Models in Prod\DM Cell Selection MDL_WEEKLY_DM_SCORES.sql";
commit;
*/
