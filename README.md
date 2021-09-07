# pg_check
Based on pg_report, but only  issues email alerts and console output, no html output format.

Typical usage: 
pg_check.py -h localhost -p 5432 -U sysdba -d mydb -l 60 -i 30 -e PROD
