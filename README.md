# pg_check
Based on pg_report, but only  issues email alerts and console output, no html output format.  Currently the things checked for:
<br/>
`waiting or blocked transactions`
<br/>
`long running queries`
<br/>
`idle in transaction`
<br/>
`high number of active connections relative to number of CPUs`
<br/><br/>

# Requirements
Python, psql command line tool
<br/><br/>

# Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -w -l 60 -i 30 -e PROD -v
<br/><br/>
`-w `     --> WAITS and LOCKS checking
<br/>
`-l 60`   --> queries running longer than 60 minutes
<br/>
`-i 30`   --> idle in transaction state for more than 30 minutes
<br/>
`-c 48`   --> number of CPUs serving this PG instance
<br/>
`-e PROD` --> User Context for PG instances that shows up in email subject line

