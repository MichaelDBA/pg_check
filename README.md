# pg_check
Based on pg_report, but only  issues email alerts and console output, no html output format.  Currently the things checked for:
<br/>
`idle in transaction`
<br/>
`long running queries`
<br/>
`waiting or blocked transactions`
<br/><br/>

# Requirements
Python, psql command line tool
<br/><br/>

# Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -l 60 -i 30 -e PROD
<br/><br/>
`-l 60`   --> queries running longer than 60 minutes
<br/>
`-i 30`   --> idle in transaction state for more than 30 minutes
<br/>
`-e PROD` --> User Context for PG instances that shows up in email subject line

