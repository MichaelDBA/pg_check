# pg_check
Based on pg_report, but only  issues email alerts and console output, no html output format.

Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -l 60 -i 30 -e PROD

-l 60 --> queries running longer than 60 minutes
-i 30 --> idle in transaction state for more than 30 minutes
-e PROD --> User Context for PG instances that shows up in email subject line
