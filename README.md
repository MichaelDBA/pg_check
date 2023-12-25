# pg_check
(c) 2018-2023 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com

# Overview
Based on pg_report, but only  issues email alerts/slack channel msgs and console output, no html output format.  Currently the things checked for:
<br/>
`PG host is accepting connections`
<br/>
`Waiting or blocked transactions`
<br/>
`Long running queries`
<br/>
`Idle in transaction`
<br/>
`Idle connections`
<br/>
`Linux Load state`
<br/>
`High number of active connections relative to number of CPUs`
<br/>
`Data Directory size > 75%`
<br/>
`Streaming replication state`
<br/>
`PGBouncer state`
<br/>
`PGBackrest last backup state`
<br/><br/>

# Requirements
Python, psql command line tool, postfix service

# Slack Setup: 
You need to put the slack webhook into a specific file location: **UserHomeDirectory/.slackhook**
<br/>It looks like this: <br/>https://hooks.slack.com/services/ "somekeyvalue without the quotes and leading space"

# Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -w -l 60 -i 30 -c 48 -e PROD -m -r -x -y <br/>
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
`-r`      --> Check Replication
<br/>
`-x`      --> Check PGBouncer
<br/>
`-y`      --> Check PGBackrest
<br/>
`-m`      --> Send Mail Notifications
<br/>
`-s`      --> Send Slack Notifications


