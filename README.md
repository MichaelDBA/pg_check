# pg_check
Based on pg_report, but only  issues email alerts and console output, no html output format.  Currently the things checked for:
<br/>
`idle in transaction`
<br/>
`long running queries`
<br/>
`waiting or blocked transactions`
<br/>
`high number of active connections relative to number of CPUs`
<br/><br/>

# Requirements
Python, psql command line tool
<br/><br/>

# Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -a LONGQUERY_IDLEINTRANS_CPULOAD  -l 60 -i 30 -c 48 -e PROD
<br/><br/>
`-a LONGQUERY_IDLEINTRANS_CPULOAD`   --> queries running longer than 60 minutes, idle in trans longer than 30 minutes, and high number of active connections relative to CPUs
<br/>
`-l 60`   --> queries running longer than 60 minutes
<br/>
`-i 30`   --> idle in transaction state for more than 30 minutes
<br/>
`-c 48`   --> number of CPUs serving this PG instance
<br/>
`-e PROD` --> User Context for PG instances that shows up in email subject line

