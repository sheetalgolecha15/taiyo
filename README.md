# taiyo
Covid Cases Analysis

Here the test csv file are present in archive folder.
Coronavirus_cases_daily_update: has the transaction data
owid_covid_codebook: has the metadata


The script was written in databricks with a combination of ADLS gen2.
The configuration to connect to gen2 should be written on the cluster config.


The script basically does aggreagtion on the country for cases.
And the second aprt does the analysis for cases count before and after vaccination started.
