# pg_check
(c) 2018-2022 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com

# Overview
Based on pg_report, but only  issues email alerts and console output, no html output format.  Currently the things checked for:
<br/>
`PG host is accepting connections`
<br/>
`waiting or blocked transactions`
<br/>
`long running queries`
<br/>
`idle in transaction`
<br/>
`idle connections`
<br/>
`high number of active connections relative to number of CPUs`
<br/>
`Data Directory size > 75%`
<br/>
`State of streaming replication`

<br/><br/>

# Requirements
Python, psql command line tool
<br/><br/>

# Slack Setup: 
You need to put the slack webhook into a specific file location: **UserHomeDirectory/.slackhook**
<br/>It looks like this: <br/>https://hooks.slack.com/services/ "somekeyvalue without the quotes and leading space"

# Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -w -l 60 -i 30 -c 48 -e PROD -m <br/>
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -o 2440 -e PROD -s
<br/><br/>
`-w 5 `     --> WAITS and LOCKS checking greater than number of seconds provided
<br/>
`-l 60`   --> queries running longer than 60 minutes
<br/>
`-i 30`   --> idle in transaction state for more than 30 minutes
<br/>
`-o 2440`   --> idle connections more than 2 days
<br/>
`-c 48`   --> number of CPUs serving this PG instance (not necessary if db is on localhost)
<br/>
`-e PROD` --> User Context for PG instances that shows up in email subject line
<br/>
`-m`      --> Send Mail Notifications
<br/>
`-s`      --> Send Slack Notifications






