#!/usr/bin/env python3 
#!/usr/bin/env python2
#!/usr/bin/env python
#!/usr/bin/python
### pg_check.py
###############################################################################
### COPYRIGHT NOTICE FOLLOWS.  DO NOT REMOVE
###############################################################################
### Copyright (c) 1998 - 2023 SQLEXEC LLC
###
### Permission to use, copy, modify, and distribute this software and its
### documentation for any purpose, without fee, and without a written agreement
### is hereby granted, provided that the above copyright notice and this paragraph
### and the following two paragraphs appear in all copies.
###
### IN NO EVENT SHALL SQLEXEC LLC BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
### INDIRECT SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
### ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
### SQLEXEC LLC HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
###
### SQLEXEC LLC SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
### LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
### PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
### AND SQLEXEC LLC HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
### ENHANCEMENTS, OR MODIFICATIONS.
###
###############################################################################
#
# Original Author: Michael Vitale, michaeldba@sqlexec.com
#
# Description: This python utility does email alerting based on specific input actions.
#
# Inputs: all fields are optional except database.
# -h <hostname or IP address>
# -p <PORT>
# -U <db user>
# -n <schema>
# -d <database>
# -g general boolean to do all other stuff as well
# -w <number> check waits/locks > number seconds
# -l <number> check for long running queries > number minutes
# -i <number> check for idle in trans > number minutes
# -o <number> check for idle connections > number minutes
# -e environment ID, aka, hostname, dbname, SDLC name, etc.
# -t [testing mode with testing email]
# -v [verbose output flag, mostly used for debugging]
#
# Examples: run report on entire test database and output in web format
# ./pg_check.py -h localhost -p 5413 -U postgres -n concept -d dvdrental -w 10 -l 60 -i 30 -c 4 -o 2440 -e PROD -v
#
# Requirements:
#  1. python 3
#  2. psql client
#
# Assumptions:
# 1. db user defaults to postgres if not provided as parameter.
# 2. Password must be in local .pgpass file or client authentication changed to trust or peer
# 3. psql must be in the user's path
# 4. Make sure timing and pager are turned off (see .psqlrc)
#
# Cron Job Info:
#    View cron job output: view /var/log/cron
#    source the database environment: source ~/db_catalog.ksh
#    Example cron job that does smart vacuum freeze commands for entire database every Saturday at 4am:
#    */10 * * * * ${TOOLDIR}/pg_check.py -h concept-v2-db-cluster-prod.cluster-clobpzafvq4q.us-east-1.rds.amazonaws.com -p 5432 -U sysdba -d conceptdb -w 10 -c 48 -e PROD  >> /home/centos/logs/pg_check_$(date +\%Y\%m\%d).log 2>&1
#    */10 * * * * ${TOOLDIR}/pg_check.py -h concept-v2-db-cluster-prod.cluster-clobpzafvq4q.us-east-1.rds.amazonaws.com -p 5432 -U sysdba -d conceptdb -w 10 -c 48 -e PROD
#    */30 * * * * ${TOOLDIR}/pg_check.py -h concept-v2-db-cluster-prod.cluster-clobpzafvq4q.us-east-1.rds.amazonaws.com -p 5432 -U sysdba -d conceptdb  -l 45 -i 30 -e PROD
#    0    7 * * * ${TOOLDIR}/pg_check.py -h concept-v2-db-cluster-prod.cluster-clobpzafvq4q.us-east-1.rds.amazonaws.com -p 5432 -U sysdba -d conceptdb  -o 2880 -e PROD
#
# NOTE: You may have to source the environment variables file in the crontab to get this program to work.
#          #!/bin/bash
#          source /home/user/.bash_profile
#
# TODOs:
#
# History:
# who did it         Date           did what
# ==========         =========      ==============================
# Michael Vitale     09/06/2021     Original coding using python 3.x CentOS 8.3 and PG 11.x
# Michael Vitale     09/14/2021     Modified parameter structure and some fixes
# Michael Vitale     09/27/2021     Added new functionality for idle connections
# Michael Vitale     09/28/2021     filter out DataFileRead-IO as a considered wait condition
# Michael Vitale     05/29/2022     detect cpu automatically if localhost and report it.
# Michael Vitale     12/12/2023     Major Upgrade: added slack notification method, bug fixes
# Michael Vitale     12/13/2023     Fixed PG major and minor version checking based on latest versions.
################################################################################################################
import string, sys, os, time
#import datetime
from datetime import datetime
from datetime import date

import tempfile, platform, math
from decimal import *
import smtplib
import subprocess
from subprocess import Popen, PIPE
from optparse  import OptionParser
import getpass

#############################################################################################
#globals
SUCCESS   = 0
ERROR     = -1
ERROR2    = -2
ERROR3    = -3
WARNING   = -4
DEFERRED  = 1
NOTICE    = 2
TOOLONG   = 3
HIGHLOAD  = 4
DESCRIPTION="This python utility program issues email/slack alerts for waits, locks, idle in trans, long queries."
VERSION    = 1.1
PROGNAME   = "pg_check"
ADATE      = "Dec 13, 2023"
PROGDATE   = "2023-12-13"
MARK_OK    = "[ OK ]  "
MARK_WARN  = "[WARN]  "

#############################################################################################
########################### class definition ################################################
#############################################################################################
class maint:
    def __init__(self):
    
        self.dateprogstr       = PROGDATE
        self.dateprog          = datetime.strptime(PROGDATE, "%Y-%m-%d")
        self.datenowstr        = datetime.now().strftime("%Y-%m-%d")
        self.datenow           = datetime.today()
        self.datediff          = self.datenow - self.dateprog
        
        self.genchechs         = ''
        self.waitslocks        = -1
        self.longquerymins     = -1
        self.idleintransmins   = -1
        self.idleconnmins      = -1
        self.cpus              = -1
        self.environment       = ''
        self.local             = False
        self.dbhost            = ''
        self.dbport            = 5432
        self.dbuser            = ''
        self.database          = ''
        self.testmode          = False
        self.verbose           = False
        self.connected         = False
        self.slacknotify       = False
        self.mailnotify        = False
        # slack hook found in users home dir/.slackhook file
        hookfile = os.path.expanduser("~") + '/.slackhook'
        with open(hookfile) as f:
            self.slackhook = f.readline().strip('\n')
        #print ('slackhook=%s' % self.slackhook)

        self.to                = 'michael.vitale@capgemini.com'
        #self.to                = 'michaeldba@sqlexec.com michael@vitalehouse.com'
        self.from_             = 'pgdude@noreply.com'

        self.fout              = ''
        self.connstring        = ''

        self.schemaclause      = ' '
        self.pid               = os.getpid()
        self.opsys             = ''
        self.tempfile          = ''
        self.tempdir           = tempfile.gettempdir()
        self.pgbindir          = ''
        self.pgversionmajor    = Decimal('0.0')
        self.pgversionminor    = '0.0'        
        self.programdir        = ''

        self.slaves            = []
        self.slavecnt          = 0
        self.in_recovery       = False
        self.bloatedtables     = False
        self.unusedindexes     = False
        self.freezecandidates  = False
        self.analyzecandidates = False
        self.timestartmins     = time.time() / 60

        # db config stuff
        self.archive_mode      = ''
        self.max_connections   = -1
        self.datadir           = ''
        self.waldir            = ''
        self.shared_buffers    = -1
        self.work_mem          = -1
        self.maint_work_mem    = -1
        self.eff_cache_size    = -1
        self.shared_preload_libraries = ''
        self.pg_type           = 'community'

        self.overcommit_memory = -1
        self.overcommit_ratio  = -1

    ###########################################################
    def send_mail(self, to, from_, subject, body):
        # assumes nonprintables are already removed from the body, else it will send it as an attachment and not in the body of the email!
        # msg = 'echo "%s" | mailx -s "%s" -r %s -- %s' % (body, subject, to, from_)
        rc = 0
        msg = 'echo "%s" | mailx -s "%s" %s' % (body, subject, to)
        # print ("DEBUG: msg=%s" % msg)
        if self.mailnotify:
            if self.verbose:
                print ("sending email...")
            rc = os.system(msg)
            #if rc == 0:
            #    print("email sent successfully.")
        if self.slacknotify:
          if self.verbose:
              print ("sending to slack...")
          msg2 = subject + ':' + body
          msg = 'curl --location "' + self.slackhook + '" --header "Content-Type: application/json" --data "{\"text\": \\"' + msg2 + '\\"}"'
          rc = os.system(msg)
          #print (msg)
        return rc
    
    ###########################################################
    def set_dbinfo(self, dbhost, dbport, dbuser, database, schema, genchecks, waitslocks, longquerymins, idleintransmins, idleconnmins, cpus, environment, testmode, verbose, slacknotify, mailnotify, argv):
        self.waitslocks      = waitslocks
        self.dbhost          = dbhost
        self.dbport          = dbport
        self.dbuser          = dbuser
        self.database        = database
        self.schema          = schema
        self.genchecks       = genchecks
        self.environment     = environment
        self.testmode        = testmode
        self.verbose         = verbose        

        self.slacknotify     = slacknotify
        self.mailnotify      = mailnotify
        
        
        if waitslocks == -999:
            #print("waitslocks not passed")
            pass
        elif waitslocks is None or waitslocks < 1:
            return ERROR, "Invalid waitslocks provided: %s" % waitslocks
        else:
            self.waitslocks      = waitslocks

        if cpus == -999:
            #print("cpus not passed")
            # cat /proc/cpuinfo | grep processor | wc -l
            cmd = 'cat /proc/cpuinfo | grep processor | wc -l'
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                # just pass
                print ("Unable to get CPU count.")
            else:
                self.cpus = int(results)
                #print("Cpus=%d" % self.cpus)
        elif cpus is None or cpus < 1:
            return ERROR, "Invalid CPUs provided: %s" % cpus
        else:
            self.cpus            = cpus

        if longquerymins == -999:
            #print("longquerymins not passed")
            pass
        elif longquerymins is None or longquerymins < 1:
            return ERROR, "Invalid longquerymins provided: %s" % longquerymins
        else:
            self.longquerymins   = longquerymins

        if idleintransmins == -999:
            #print("idleintransmins not passed")
            pass
        elif idleintransmins is None or idleintransmins < 1:
            return ERROR, "Invalid idleintransmins provided: %s" % idleintransmins
        else:
            self.idleintransmins = idleintransmins
            
        if idleconnmins == -999:
            #print("idleconnmins not passed")
            pass
        elif idleconnmins is None or idleconnmins < 1:
            return ERROR, "Invalid idleconnmins provided: %s" % idleconnmins
        else:
            self.idleconnmins = idleconnmins
        
        if self.testmode:
            #self.to  = 'michaeldba@sqlexec.com '
            self.to  = 'michael.vitale@capgemini.com'
            print("testing mode")
        else:
            #self.to  = 'michaeldba@sqlexec.com michael@vitalehouse.com'
            self.to  = 'michael.vitale@capgemini.com'
             

        # process the schema or table elements
        total   = len(argv)
        cmdargs = str(argv)

        if os.name == 'posix':
            self.opsys = 'posix'
            self.dir_delim = '/'
        elif os.name == 'nt':
            self.opsys = 'nt'
            self.dir_delim = '\\'
        else:
            return ERROR, "Unsupported platform."
            
        self.workfile          = "%s%s%s_stats.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.workfile_deferred = "%s%s%s_stats_deferred.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.tempfile          = "%s%s%s_temp.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.reportfile        = "%s%s%s_report.txt" % (self.tempdir, self.dir_delim, self.pid)

        # construct the connection string that will be used in all database requests
        # do not provide host name and/or port if not provided
        if self.dbhost != '':
            self.connstring = " -h %s " % self.dbhost
        if self.database != '':
            self.connstring += " -d %s " % self.database
        if self.dbport != '':
            self.connstring += " -p %s " % self.dbport
        if self.dbuser != '':
            self.connstring += " -U %s " % self.dbuser
        if self.schema != '':
            self.schemaclause = " and n.nspname = '%s' " % self.schema

        # check if local connection for automatic checking of cpus, mem, etc.
        if 'localhost' in self.dbhost or '127.0.0.1' in self.dbhost or dbhost == '':
            # appears to be local host
            self.local = True

        if self.verbose:
            print ("The total numbers of args passed to the script: %d " % total)
            print ("Args list: %s " % cmdargs)
            print ("connection string: %s" % self.connstring)

        self.programdir = sys.path[0]

        # Make sure psql is in the path
        if self.opsys == 'posix':
            cmd = "which psql"
        else:
            # assume windows
            cmd = "where psql"
        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "Unable to determine if psql is in path. rc=%d results=%s" % (rc,results)
            return rc, errors
        if 'psql' not in results:
            msg = "psql must be in the path. rc=%d, results=%s" % (rc, results)
            return ERROR, msg

        pos = results.find('psql')
        if pos > 0:
            self.pgbindir = results[0:pos]

        rc, results = self.get_configinfo()
        if rc != SUCCESS:
            errors = "rc=%d results=%s" % (rc,results)
            return rc, errors

        rc, results = self.get_pgversion()
        if rc != SUCCESS:
            return rc, results

        return SUCCESS, ''


    ###########################################################
    def cleanup(self):
        if self.connected:
            # do something here later if we enable a db driver
            self.connected = false
        # print ("deleting temp file: %s" % self.tempfile)
        try:
            os.remove(self.tempfile)
        except OSError:
            pass
        return

    ###########################################################
    def getnow(self):
        now = datetime.now()
        adate = str(now)
        parts = adate.split('.')
        return parts[0]

    ###########################################################
    def getfilelinecnt(self, afile):
        return sum(1 for line in open(afile))

    ###########################################################
    def convert_humanfriendly_to_MB(self, humanfriendly):

        # assumes input in form: 10GB, 500 MB, 200 KB, 1TB
        # returns value in megabytes
        hf = humanfriendly.upper()
        valueMB = -1
        if 'TB' in (hf):
            pos = hf.find('TB')
            valueMB = int(hf[0:pos]) * (1024*1024)
        elif 'GB' in (hf):
            pos = hf.find('GB')
            value = hf[0:pos]
            valueMB = int(hf[0:pos]) * 1024
        elif 'MB' in (hf):
            pos = hf.find('MB')
            valueMB = int(hf[0:pos]) * 1
        elif 'KB' in (hf):
            pos = hf.find('KB')
            valueMB = round(float(hf[0:pos]) / 1024, 2)

        valuefloat = "%.2f" % valueMB
        return Decimal(valuefloat)


    ###########################################################
    def writeout(self,aline):
        if self.fout != '':
            aline = aline + "\r\n"
            self.fout.write(aline)
        else:
            # default to standard output
            print (aline)
        return

    ###########################################################
    def get_configinfo(self):

        sql = "show all"

        #print("conn=%s" % self.connstring)
        cmd = "psql %s -At -X -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            # let calling function report the error
            errors = "Unable to get config info: %d %s\nsql=%s\n" % (rc, results, sql)
            #aline = "%s" % (errors)
            #self.writeout(aline)
            return rc, errors

        f = open(self.tempfile, "r")
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            aline = line.strip()
            if len(aline) < 1:
                continue

            # v2.2 fix: things like "Timing is On" can appear as a line so bypass
            if aline == 'Timing is on.' or aline == 'Timing is off.' or aline == 'Pager usage is off.' or aline == 'Pager is used for long output.' or ':activity' in aline or 'Time: ' in aline:
                continue
                
            # print ("DEBUG:  aline=%s" % (aline))
            fields = aline.split('|')
            name = fields[0].strip()
            setting = fields[1].strip()
            #print ("name=%s  setting=%s" % (name, setting))

            if name == 'data_directory':
                self.datadir = setting
                if self.pgversionmajor > Decimal('9.6'):
                    self.waldir = "%s/pg_wal" % self.datadir                
                else:
                    self.waldir = "%s/pg_xlog" % self.datadir        
                
                # for pg rds version, 9.6,  "show all" command does not have shared_preload_libraries! so rely on data_directory instead
                if 'rdsdbdata' in self.datadir:
                    self.pg_type = 'rds'                
                # heroku indicator using aws in the background
                elif self.datadir == '/database':
                    self.pg_type = 'rds'                
            elif name == 'archive_mode':
                self.archive_mode = setting
            elif name == 'max_connections':
                self.max_connections = int(setting)
            elif name == 'shared_buffers':
                # shared_buffers in 8kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                # self.shared_buffers = int(setting) / 8192
                rc = self.convert_humanfriendly_to_MB(setting)
                self.shared_buffers = rc
            elif name == 'maintenance_work_mem':
                # maintenance_work_mem in kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                # self.maint_work_mem = int(setting) / 1024
                rc = self.convert_humanfriendly_to_MB(setting)
                self.maint_work_mem = rc
            elif name == 'work_mem':
                # work_mem in kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                #self.work_mem = int(setting) / 1024
                rc = self.convert_humanfriendly_to_MB(setting)
                self.work_mem = rc
            elif name == 'effective_cache_size':
                # effective_cache_size in 8 kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                rc = self.convert_humanfriendly_to_MB(setting)
                self.eff_cache_size = rc
            elif name == 'shared_preload_libraries':
                # we only care that it is loaded, not necessarily created
                # for pg rds version, 9.6,  "show all" command does not have shared_preload_libraries! so rely on data_directory instead
                self.shared_preload_libraries = setting  
                if 'rdsutils' in self.shared_preload_libraries:
                    self.pg_type = 'rds'
            elif name == 'rds.extensions':
                self.pg_type = 'rds'

        f.close()

        if self.verbose:
            print ("shared_buffers = %d  maint_work_mem = %d  work_mem = %d  shared_preload_libraries = %s" % (self.shared_buffers, self.maint_work_mem, self.work_mem, self.shared_preload_libraries))

        return SUCCESS, results

    ###########################################################
    def executecmd(self, cmd, expect):
        if self.verbose:
            print ("executecmd --> %s" % cmd)

        # NOTE: try and catch does not work for Popen
        try:
            # Popen(args, bufsize=0, executable=None, stdin=None, stdout=None, stderr=None, preexec_fn=None, close_fds=False, shell=False, cwd=None, env=None, universal_newlines=False, startupinfo=None, creationflags=0)
            if self.opsys == 'posix':
                p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, executable="/bin/bash")
            else:
                p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            values2, err2 = p.communicate()

        except exceptions.OSError as e:
            print ("exceptions.OSError Error",e)
            return ERROR, "Error(1)"
        except BaseException as e:
            print ("BaseException Error",e)
            return ERROR, "Error(2)"
        except OSError as e:
            print ("OSError Error", e)
            return ERROR, "Error(3)"
        except RuntimeError as e:
            print ("RuntimeError", e)
            return ERROR, "Error(4)"
        except ValueError as e:
            print ("Value Error", e)
            return ERROR, "Error(5)"
        except Exception as e:
            print ("General Exception Error", e)
            return ERROR, "Error(6)"
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            return ERROR, "Error(7)"

        if err2 is None or len(err2) == 0:
            err = ""
        else:
            # python 3 returns values and err in byte format so convert accordingly
            err = bytes(err2).decode('utf-8')
            
        if values2 is None or len(values2) == 0:
            values = ""
        else:
            # python 3 returns values and err in byte format so convert accordingly
            values = bytes(values2).decode('utf-8')

        values = values.strip()
                
        rc = p.returncode
        if self.verbose:
            print ("rc=%d  values=***%s***  errors=***%s***" % (rc, values, err))

        if rc == 1 or rc == 2:
            return ERROR2, err
        elif rc == 127:
            return ERROR2, err
        elif err != "":
            # do nothing since INFO information is returned here for analyze commands
            # return ERROR, err
            return SUCCESS, err
        elif values == "" and expect == True:
            return ERROR2, values
        elif rc != SUCCESS:
            # print or(stderr_data)
            return rc, err
        elif values == "" and expect:
            return ERROR3, 'return set is empty'
        else:
            return SUCCESS, values


    ###########################################################
    def get_pgversion(self):

        # v 2.1 fix: expected output --> 10.15-10.
        #sql = "select substring(foo.version from 12 for 3) from (select version() as major) foo, substring(version(), 12, position(' ' in substring(version(),12))) as minor"
        #sql = "select substring(version(), 12, position(' ' in substring(version(),12)))"
        sql = "select  trim(substring(version(), 12, position(' ' in substring(version(),12)))) || '-' || substring(foo.major from 12 for 3)as major  from (select version() as major) foo"
        
        # do not provide host name and/or port if not provided
        cmd = "psql %s -At -X -c \"%s\" " % (self.connstring, sql)
        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "%s\n" % (results)
            aline = "%s" % (errors)

            self.writeout(aline)
            return rc, errors

        # with version 10, major version format changes from x.x to x, where x is a 2 byte integer, ie, 10, 11, etc.
        # values = bytes(values2).decode('utf-8')
        results = str(results)
        parsed = results.split('-')
        
        amajor = parsed[1]
        self.pgversionminor = parsed[0]
        
        pos = amajor.find('.')
        if pos == -1:
            # must be a beta or rc candidate version starting at version 10 since the current version is 10rc1
            self.pgversionmajor =  Decimal(amajor[:2])
        else:
            self.pgversionmajor = Decimal(amajor)
        
        #print ("majorversion = %.1f  minorversion = %s" % (self.pgversionmajor, self.pgversionminor))
        return SUCCESS, str(results)

    ###########################################################
    def get_readycnt(self):
        
        # we cannot handle cloud types like AWS RDS
        if self.pg_type == 'rds':
            return SUCCESS, '0'
        
        # version 10 replaces pg_xlog with pg_wal directory
        if self.pgversionmajor > Decimal('9.6'):
            xlogdir = "%s/pg_wal/archive_status" % self.datadir                
        else:
            xlogdir = "%s/pg_xlog/archive_status" % self.datadir        
        
        sql = "select count(*) from (select pg_ls_dir from pg_ls_dir('%s') where pg_ls_dir ~ E'^[0-9A-F]{24}.ready$') as foo" % xlogdir

        # do not provide host name and/or port if not provided
        cmd = "psql %s -At -X -c \"%s\" " % (self.connstring, sql)

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "%s\n" % (results)
            aline = "%s" % (errors)

            self.writeout(aline)
            return rc, errors

        return SUCCESS, str(results)

    ###########################################################
    def get_datadir(self):

        sql = "show data_directory"

        # do not provide host name and/or port if not provided
        cmd = "psql %s -At -X -c \"%s\" " % (self.connstring, sql)

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "%s\n" % (results)
            aline = "%s" % (errors)

            self.writeout(aline)
            return rc, errors

        return SUCCESS, str(results)

    ###########################################################
    def get_pgbindir(self):

        if self.opsys == 'posix':
            cmd = "pg_config | grep BINDIR"
        else:
            cmd = "pg_config | find \"BINDIR\""

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            # don't consider failure unless bindir not already populated by "which psql" command that executed earlier
            if self.pgbindir == "":
                errors = "unable to get PG Bind Directory.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            else:
                return SUCCESS, self.pgbindir

        results = results.split('=')
        self.pgbindir   = results[1].strip()

        if self.verbose:
            print ("PG Bind Directory = %s" % self.pgbindir)

        return SUCCESS, str(results)


    ###########################################################
    def do_report(self):

        if self.waitslocks > 0:
            ##########################################################
            # Get lock waiting transactions where wait is > input seconds
            ##########################################################
            if self.pgversionmajor < Decimal('9.2'):
              # select procpid, datname, usename, client_addr, now(), query_start, substring(current_query,1,100), now() - query_start as duration from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds';
                sql = "select count(*) from pg_stat_activity where waiting is true and now() - query_start > interval '%d seconds'" % self.waitslocks
                sql2 = "TODO"
            elif self.pgversionmajor < Decimal('9.6'):
              # select pid, datname, usename, client_addr, now(), query_start, substring(query,1,100), now() - query_start as duration from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds';
                sql1 = "select count(*) from pg_stat_activity where waiting is true and now() - query_start > interval '%d seconds'" % self.waitslocks
                sql2 = "TODO"
            else:
                # new wait_event column replaces waiting in 9.6/10
                # v2.2 fix: add backend_type qualifier to not consider walsender

                # filter out DataFileRead-IO
                #sql1 = "select count(*) from pg_stat_activity where wait_event is NOT NULL and state = 'active' and backend_type <> 'walsender' and now() - query_start > interval '%d seconds'" % self.waitslocks
                sql1 = "select count(*) from pg_stat_activity where wait_event is NOT NULL AND wait_event <> 'DataFileRead' and state = 'active' and backend_type <> 'walsender' and now() - query_start > interval '%d seconds'" % self.waitslocks
                sql2 = "select 'db=' || datname || '  user=' || usename || '  appname=' || application_name || '  waitinfo=' || wait_event || '-' || wait_event_type || " \
                       "'  duration=' || cast(EXTRACT(EPOCH FROM (now() - query_start)) as integer) || '\n'" \
                       "'sql=' || regexp_replace(replace(regexp_replace(query, E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') || '\n'" \
                       "from pg_stat_activity where wait_event is NOT NULL and state = 'active' and backend_type <> 'walsender' and now() - query_start > interval '%d seconds'" % self.waitslocks
                sql3 = "SELECT '\n\nblocked_pid =' || rpad(cast(blocked_locks.pid as varchar),7,' ') || ' blocked_user=' || blocked_activity.usename || " \
                    "'\nblocking_pid=' || rpad(cast(blocking_locks.pid as varchar), 7, ' ') || 'blocking_user=' || blocking_activity.usename || '\n' ||" \
                    "'blocked_query =' || regexp_replace(replace(regexp_replace(blocked_activity.query, E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') || '...\n' ||" \
                    "'blocking_query=' || regexp_replace(replace(regexp_replace(blocking_activity.query, E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') || '...\n\n' FROM pg_catalog.pg_locks blocked_locks " \
                    "JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype AND " \
                    "blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation AND blocking_locks.page IS NOT DISTINCT " \
                    "FROM blocked_locks.page AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid AND " \
                    "blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid AND blocking_locks.objid IS NOT DISTINCT " \
                    "FROM blocked_locks.objid AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid AND blocking_locks.pid != blocked_locks.pid " \
                    "JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid WHERE NOT blocked_locks.GRANTED"
                    
            cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql1)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get count of blocked queries: %d %s\nsql=%s\n" % (rc, results, sql1)
                return rc, errors
            blocked_queries_cnt = int(results)
            if blocked_queries_cnt == 0:
                marker = MARK_OK
                msg = "No \"Waiting/Blocked queries\" longer than %d seconds were detected." % self.waitslocks
            else:
                marker = MARK_WARN
                msg = "%d \"Waiting/Blocked queries\" longer than %d seconds were detected." % (blocked_queries_cnt, self.waitslocks)
                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql2)
                rc, results2 = self.executecmd(cmd, False)
                if rc != SUCCESS:
                    print ("Unable to get waiting or blocked queries(A): %d %s\nsql=%s\n" % (rc, results2, sql2))            
                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql3)
                rc, results3 = self.executecmd(cmd, False)
                if rc != SUCCESS:
                    print ("Unable to get waiting or blocked queries(B): %d %s\nsql=%s\n" % (rc, results2, sql3))
                    
                subject = '%d %s Waiting/BLocked SQL(s) Detected' % (blocked_queries_cnt, self.environment)
                if results2 is None or results2.strip() == '':
                    results2 = ''
                if results3 is None or results3.strip() == '':
                    results3 = ''                    
                if self.verbose:
                    print("results2=%s" % results2)
                    print("results3=%s" % results3)
                    print("")
                    print("total results=%s" % results2 + '\r\n' + results3)
                # /r makes body disappear!
                if results2.strip() == '' and results3.strip() == '':
                    # then must have gone away so don't report anything
                    msg = "%d \"Waiting/Blocked queries\" longer than %d seconds were detected but details not available anymore." % (blocked_queries_cnt, self.waitslocks)
                    if self.verbose:
                        print("%d waits/locks detected, but details are no longer available." % blocked_queries_cnt)
                else:
                    #rc = self.send_mail(self.to, self.from_, subject, results2+ '\r\n' + results3)
                    rc = self.send_mail(self.to, self.from_, subject, results2 + '\n' + results3)
                    if rc != 0:
                        printit("mail error")
                        return 1
            print (marker+msg)
            
        if self.idleintransmins   > 0:
            #######################################################################
            # get existing "idle in transaction" connections longer than 10 minutes
            #######################################################################
            # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
                #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+
            if self.pgversionmajor < Decimal('9.2'):
                # select substring(current_query,1,50), round(EXTRACT(EPOCH FROM (now() - query_start))), now(), query_start  from pg_stat_activity;
                sql1 = "select count(*) from pg_stat_activity where current_query ilike \'<IDLE> in transaction%%\' and round(EXTRACT(EPOCH FROM (now() - query_start))) > %d" % self.idleintransmins
                sql2 = "select datname, usename, application_name from pg_stat_activity where current_query ilike \'idle in transaction\' and round(EXTRACT(EPOCH FROM (now() - query_start))) > %d" % self.idleintransmins
            else:
                # select substring(query,1,50), round(EXTRACT(EPOCH FROM (now() - query_start))), now(), query_start, state  from pg_stat_activity;
                sql1 = "select count(*) from pg_stat_activity where state = \'idle in transaction\' and round(EXTRACT(EPOCH FROM (now() - query_start))) / 60 > %d" % self.idleintransmins
                sql2 = "select 'pid=' || pid || '  db=' || datname || '  user=' || usename || '  app=' || coalesce(application_name, 'N/A') || '  clientip=' || client_addr || '  duration=' || round(round(EXTRACT(EPOCH FROM (now() - query_start))) / 60) || ' mins' from pg_stat_activity where state = \'idle in transaction\' and round(EXTRACT(EPOCH FROM (now() - query_start))) / 60 > %d" % self.idleintransmins
                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql1)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get count of idle in transaction connections: %d %s\nsql=%s\n" % (rc, results, sql1)
                return rc, errors
            idle_in_transaction_cnt = int(results)

            if idle_in_transaction_cnt == 0:
                marker = MARK_OK
                msg = "No \"idle in transaction\" longer than %d minutes were detected." % self.idleintransmins
            else:
                marker = MARK_WARN
                msg = "%d \"idle in transaction\" longer than %d minutes were detected." % (idle_in_transaction_cnt, self.idleintransmins)

                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql2)
                rc, results2 = self.executecmd(cmd, False)
                if rc != SUCCESS:
                    print ("Unable to get idle in transaction queries: %d %s\nsql=%s\n" % (rc, results2, sql2))
                subject = '%d %s Idle In Trans SQL(s) detected longer than %d minutes' % (idle_in_transaction_cnt, self.environment, self.idleintransmins)            
                rc = self.send_mail(self.to, self.from_, subject, results2)
                if rc != 0:
                    printit("mail error")
                    return 1
            print (marker+msg)

        if self.longquerymins > 0:
            ######################################
            # Get long running queries > 5 minutes (default
            ######################################
            # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
            #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+
            if self.pgversionmajor < Decimal('9.2'):
                # select procpid,datname,usename, client_addr, now(), query_start, substring(current_query,1,100), now() - query_start as duration from pg_stat_activity where current_query not ilike '<IDLE%' and current_query <> ''::text and now() - query_start > interval '5 minutes';
                sql1 = "select count(*) from pg_stat_activity where current_query not ilike '<IDLE%' and current_query <> ''::text and now() - query_start > interval '%d minutes'" % self.longquerymins
                sql2 = "select 'db=' || datname || '  user=' || usename || '  appname=' || application_name || '  sql=' || query from pg_stat_activity where current_query not ilike '<IDLE%%' and current_query <> ''::text and now() - query_start > interval '%d minutes'" % self.longquerymins
            else:
                # select pid,datname,usename, client_addr, now(), state, query_start, substring(query,1,100), now() - query_start as duration from pg_stat_activity where state not ilike 'idle%' and query <> ''::text and now() - query_start > interval '%d minutes'" % self.longquerymins
                sql1 = "select count(*) from pg_stat_activity where backend_type not in ('walsender') and state not ilike 'idle%%' and query <> ''::text and now() - query_start > interval '%s minutes'" % self.longquerymins
                sql2 = "select 'pid=' || pid || '  db=' || datname || '  user=' || usename || '  appname=' || coalesce(application_name, 'N/A') || '  minutes=' || " \
                       "(case when state in ('active','idle in transaction') then cast(EXTRACT(EPOCH FROM (now() - query_start)) as integer) / 60 else -1 end) || '\n' ||" \
                       "'sql=' || regexp_replace(replace(regexp_replace(query, E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') || '\n\n'" \
                       "from pg_stat_activity where backend_type not in ('walsender') and state not ilike 'idle%%' and query <> ''::text and now() - query_start > interval '%d minutes'" % self.longquerymins

            cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql1)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get count of long running queries: %d %s\nsql=%s\n" % (rc, results, sql1)
                return rc, errors
            long_queries_cnt = int(results)
            if long_queries_cnt == 0:
                marker = MARK_OK
                msg = "No \"long running queries\" longer than %d minutes were detected." % self.longquerymins
                print (marker+msg)
            else:
                # get the actual sqls:
                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql2)
                rc, results2 = self.executecmd(cmd, False)
                if rc != SUCCESS:
                    errors = "Unable to get long running queries: %d %s\nsql=%s\n" % (rc, results2, sql2)
                    print (errors)
                    return rc, errors
           
                marker = MARK_WARN
                msg = "%d \"long running queries\" longer than %d minutes were detected." % (long_queries_cnt, self.longquerymins)
                print (marker+msg)      
                subject = '%d %s Long Running SQL(s) Detected longer than %d minutes' % (long_queries_cnt, self.environment, self.longquerymins)
                rc = self.send_mail(self.to, self.from_, subject, results2)
                if rc != 0:
                    printit("mail error")
                    return 1

        if self.cpus > 0 or self.local:
            ######################################
            # Get cpu load info
            # db.r5.12xlarge = 48
            ######################################

            sql = "select count(*) as active from pg_stat_activity where state in ('active', 'idle in transaction')"
            cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get count of active connections: %d %s\nsql=%s\n" % (rc, results, sql1)
                return rc, errors
            active_cnt = int(results)
            # formula is (#cpus * 2) + (#cpus / 2)
            cpusaturation = round(self.cpus * 2.5)
            loadpct = round(active_cnt / cpusaturation, 2) * 100
            loadint = int(loadpct)
            #print("activecnt=%d  cpus=%d  cpusaturation=%4.1f   loadpct=%4.2f  loadint=%d" % (active_cnt, self.cpus,cpusaturation, loadpct, loadint))
            if loadpct <= 80.0:
                marker = MARK_OK
                msg = "No \"high number of active connections\" detected:%d" % active_cnt
            else:
                marker = MARK_WARN
                subject = 'High CPU load detected.'
                msg = "\"High number of active connections\" detected:%d  Implied load: %d%%" % (active_cnt, loadint)
                rc = self.send_mail(self.to, self.from_, subject, msg)
                if rc != 0:
                    printit("mail error")
                    return 1
            print (marker+msg)            

        if self.idleconnmins   > 0:
            #############################################################
            # get existing idle connections longer than specified minutes
            #############################################################
            # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
                #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+
            if self.pgversionmajor < Decimal('9.2'):
                print ("Action not implemented for PG Versions < 9.1 and older.")
                return 1
            else:
                # NOTE: filter condition based on IMO customization for "ggs"
                sql1 = \
                    "select count(*) from pg_stat_activity where state = 'idle' and usename <> 'ggs' and cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) / 60 > %d" % self.idleconnmins
                    
                '''                    
                select 'pid=' || pid || '  db=' || coalesce(datname,'N/A') || '  user=' || coalesce(usename, 'N/A') || '  app=' || coalesce(application_name, 'N/A') || '  clientip=' || client_addr || '  state=idle' || 
                '  backend_type=' || (case when backend_type = 'logical replication launcher' then 'logical rep launcher' when backend_type = 'autovacuum launcher' then 'autovac launcher' when backend_type = 'autovacuum worker' then 'autovac wrkr' else backend_type end) || 
                '  backend_start=' || to_char(backend_start, 'YYYY-MM-DD HH24:MI:SS') || 
                '  conn mins=' || cast(EXTRACT(EPOCH FROM (now() - backend_start)) / 60 as integer) || 
                '  idle mins=' || cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) / 60 as idle_mins 
                FROM pg_stat_activity WHERE state in ('idle') and usename <> 'ggs' and cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) / 60 > 200 order by cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) desc;
                '''                    
                
                sql2 = \
                    "select 'pid=' || pid || '  db=' || coalesce(datname,'N/A') || '  user=' || coalesce(usename, 'N/A') || '  app=' || coalesce(application_name, 'N/A') || '  clientip=' || client_addr || '  state=idle' || " \
                    " '  backend_type=' || (case when backend_type = 'logical replication launcher' then 'logical rep launcher' when backend_type = 'autovacuum launcher' then 'autovac launcher' when backend_type = 'autovacuum worker' then 'autovac wrkr' else backend_type end) || " \
                    " '  backend_start=' || to_char(backend_start, 'YYYY-MM-DD HH24:MI:SS') || " \
                    " '  conn mins=' || cast(EXTRACT(EPOCH FROM (now() - backend_start)) / 60 as integer) || " \
                    " '  idle mins=' || cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) / 60 as idle_mins " \
                    " FROM pg_stat_activity WHERE state in ('idle') and usename <> 'ggs' and cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) / 60 > %d order by cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) desc" % self.idleconnmins
                
                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql1)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get count of idle connections: %d %s\nsql=%s\n" % (rc, results, sql1)
                return rc, errors
            idle_conns = int(results)

            if idle_conns == 0:
                marker = MARK_OK
                msg = "No \"idle connections\" longer than %d minutes were detected." % self.idleconnmins
            else:
                marker = MARK_WARN
                msg = "%d \"idle connections\" longer than %d minutes were detected." % (idle_conns, self.idleconnmins)

                cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql2)
                rc, results2 = self.executecmd(cmd, False)
                if rc != SUCCESS:
                    print ("Unable to get idle connection: %d %s\nsql=%s\n" % (rc, results2, sql2))
                subject = '%d %s Idle connection(s) detected longer than %d minutes' % (idle_conns, self.environment, self.idleconnmins)            
                rc = self.send_mail(self.to, self.from_, subject, results2)
                if rc != 0:
                    printit("mail error")
                    return 1
            print (marker+msg)

        if not self.genchecks:
            return SUCCESS, ""
                        
        #####################################
        # analyze pg major and minor versions
        #####################################
        if self.pgversionmajor < Decimal('11.0'):
            marker = MARK_WARN
            msg = "Unsupported major version detected: %.1f.  Please upgrade ASAP." % self.pgversionmajor
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Major Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        elif self.pgversionmajor < Decimal('16.0'):
            marker = MARK_WARN        
            msg = "Current PG major version (%s) is not the latest.  Consider upgrading to 16." % self.pgversionmajor
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Major Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                        
        else:
            marker = MARK_OK        
            msg = "Current PG major version (%s) is the latest.  No major upgrade necessary." % self.pgversionmajor        
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">PG Major Version Summary</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"        

        print (marker+msg)        
        
        # latest versions: 16.1, 15.5, 14.10, 13.13, 12.17, 11.22, 10.23, 9.6.24
        #print("latest version: %s" % self.pgversionmajor)
        if self.pgversionmajor > Decimal('9.5'):
            if self.datediff.days > 120:
                # probably a newer minor version is already out since these minor versions were last updated in the program
                marker = MARK_WARN
                msg = "Current version: %s.  Please upgrade to latest minor version." % self.pgversionminor
            elif self.pgversionmajor == Decimal('9.6') and self.pgversionminor < '9.6.24':
                marker = MARK_WARN
                msg = "Current version: %s.  Please upgrade to last minor version, 9.6.24." % self.pgversionminor
            elif self.pgversionmajor == Decimal('10.0') and self.pgversionminor < '10.23':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to last minor version, 10.23." % self.pgversionminor
            elif self.pgversionmajor == Decimal('11.0') and self.pgversionminor < '11.22':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 11.22." % self.pgversionminor
            elif self.pgversionmajor == Decimal('12.0') and self.pgversionminor < '12.17':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 12.17." % self.pgversionminor
            elif self.pgversionmajor == Decimal('13.0') and self.pgversionminor < '13.13':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 13.13." % self.pgversionminor
            elif self.pgversionmajor == Decimal('14.0') and self.pgversionminor < '14.10':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 14.10." % self.pgversionminor
            elif self.pgversionmajor == Decimal('15.0') and self.pgversionminor < '15.5':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 15.5." % self.pgversionminor                
            elif self.pgversionmajor == Decimal('16.0') and self.pgversionminor < '16.1':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 16.1." % self.pgversionminor                
            else:
                marker = MARK_OK        
                msg = "Current PG minor version is the latest (%s). No minor upgrade necessary." % self.pgversionminor        

            print (marker+msg)                

        #####################
        # get cache hit ratio
        #####################
        # SELECT datname, blks_read, blks_hit, round((blks_hit::float/(blks_read+blks_hit+1)*100)::numeric, 2) as cachehitratio FROM pg_stat_database ORDER BY datname, cachehitratio
        sql = "SELECT blks_read, blks_hit, round((blks_hit::float/(blks_read+blks_hit+1)*100)::numeric, 2) as cachehitratio FROM pg_stat_database where datname = '%s' ORDER BY datname, cachehitratio" % self.database
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get database cache hit ratio: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        cols = results.split('|')
        blks_read   = int(cols[0].strip())
        blks_hit    = int(cols[1].strip())
        cache_ratio = Decimal(cols[2].strip())
        if cache_ratio < Decimal('70.0'):
            marker = MARK_WARN
            msg = "low cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
        elif cache_ratio < Decimal('90.0'):
            marker = MARK_WARN        
            msg = "Moderate cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
        else:
            marker = MARK_OK
            msg = "High cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
        print (marker+msg)

        ##########################
        # shared_preload_libraries
        ##########################
        if 'pg_stat_statements' not in self.shared_preload_libraries:
            marker = MARK_WARN
            msg = "pg_stat_statements extension not loaded."
        else:
            marker = MARK_OK
            msg = "pg_stat_statements loaded"
        print (marker+msg)



        ######################################################
        # get connection counts and compare to max connections
        ######################################################
        sql = "select count(*) from pg_stat_activity"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get count of current connections: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        conns = int(results)
        result = float(conns) / self.max_connections
        percentconns = int(math.floor(result * 100))
        if self.verbose:
            print ("Max connections = %d   Current connections = %d   PctConnections = %d" % (self.max_connections, conns, percentconns))

        if percentconns > 80:
            # 80 percent is the hard coded threshold
            marker = MARK_WARN        
            msg = "Current connections (%d) are greater than 80%% of max connections (%d) " % (conns, self.max_connections)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Connections</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_OK
            msg = "Current connections (%d) are not too close to max connections (%d) " % (conns, self.max_connections)
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Connections</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        print (marker+msg)

        ###########################################################################################################################################
        # database conflicts: only applies to PG versions greater or equal to 9.1.  9.2 has additional fields of interest: deadlocks and temp_files
        ###########################################################################################################################################
        if self.pgversionmajor < Decimal('9.1'):
            msg = "No database conflicts found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Database Conflicts</font></td><td width=\"75%\"><font color=\"blue\">N/A</font></td></tr>"
            return SUCCESS, ""
            print (msg)
            return SUCCESS, ""

        if self.pgversionmajor < Decimal('9.2'):
            sql="select datname, conflicts from pg_stat_database where datname = '%s'" % self.database
        else:
            sql="select datname, conflicts, deadlocks, temp_files, temp_bytes from pg_stat_database where datname = '%s'" % self.database
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get database conflicts: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        cols = results.split('|')
        database   = cols[0].strip()
        conflicts  = int(cols[1].strip())
        deadlocks  = -1
        temp_files = -1
        temp_bytes = -1
        if len(cols) > 2:
            deadlocks  = int(cols[2].strip())
            temp_files = int(cols[3].strip())
            temp_bytes = int(cols[4].strip())            

        #  if self.verbose:
        #      print
        if conflicts > 0 or deadlocks > 0 or temp_files > 0:
            marker = MARK_WARN
            msg = "Database conflicts found: database=%s  conflicts=%d  deadlocks=%d  temp_files=%d  temp_bytes=%d" % (database, conflicts, deadlocks, temp_files, temp_bytes)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Database Conflicts (deadlocks, Query disk spillover, Standby cancelled queries)</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_OK
            msg = "No database conflicts found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Database Conflicts (deadlocks, Query disk spillover, Standby cancelled queries</font></td><td width=\"75%\"><font color=\"blue\">No database conflicts found.</font></td></tr>"
        print (marker+msg)

        ###############################################################################################################
        # Check for checkpoint frequency unless we are in rds mode
        # NOTE: Checkpoints should happen every few minutes, not less than 5 minutes and not more than 15-30 minutes
        #       unless recovery time is not a priority and High I/O SQL workload is in which case 1 hour is reasonable.
        ###############################################################################################################
        if self.pg_type != 'rds':
            sql = "SELECT total_checkpoints, seconds_since_start / total_checkpoints / 60 AS minutes_between_checkpoints, checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time FROM (SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) AS seconds_since_start, (checkpoints_timed+checkpoints_req) AS total_checkpoints, checkpoints_timed, checkpoints_req, checkpoint_write_time / 1000 as checkpoint_write_time, checkpoint_sync_time / 1000 as checkpoint_sync_time FROM pg_stat_bgwriter) AS sub"
            cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get checkpoint frequency: %d %s\nsql=%s\n" % (rc, results, sql)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors

            cols = results.split('|')
            total_checkpoints     = int(cols[0].strip())
            minutes               = Decimal(cols[1].strip())
            checkpoints_timed     = int(cols[2].strip())
            checkpoints_req       = int(cols[3].strip())
            checkpoint_write_time = int(float(cols[4].strip()))
            checkpoint_sync_time  = int(float(cols[5].strip()))        \
            # calculate average checkpoint time
            avg_checkpoint_seconds = ((checkpoint_write_time + checkpoint_sync_time) / (checkpoints_timed + checkpoints_req))

            if minutes < Decimal('5.0'):
                marker = MARK_WARN
                msg = "Checkpoints are occurring too fast, every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            elif minutes > Decimal('60.0'):
                marker = MARK_WARN
                msg = "Checkpoints are occurring too infrequently, every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            else:
                marker = MARK_OK
                msg = "Checkpoints are occurring every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            print (marker+msg)

        ####################################
        # Check some postgresql config parms
        ####################################
        sql = "with summary as (select name, setting from pg_settings where name in ('autovacuum', 'checkpoint_completion_target', 'data_checksums', 'idle_in_transaction_session_timeout', 'log_checkpoints', 'log_lock_waits',  'log_min_duration_statement', 'log_temp_files', 'shared_preload_libraries', 'track_activity_query_size') order by 1 ) select setting from summary order by name"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)        
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get configuration parameters: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors        
        # since we have multiple rows, we split based on carriage return, not pipe when one row is returned
        cols = results.split('\n')

        autovacuum                           = cols[0].strip()
        checkpoint_completion_target         = Decimal(cols[1].strip())
        data_checksums                       = cols[2].strip()
        idle_in_transaction_session_timeout = int(cols[3].strip())
        log_checkpoints                     = cols[4].strip()
        log_lock_waits                      = cols[5].strip()
        log_min_duration_statement          = int(cols[6].strip())
        log_temp_files                      = cols[7].strip()
        shared_preload_libraries            = cols[8].strip()
        track_activity_query_size           = int(cols[9].strip())

        #print ("autovac=%s  chk_target=%s  sums=%s  idle=%s  log_checkpoints=%s  log_locks= %s  log_min=%s  log_temp=%s  shared=%s  track=%s" \
        #      % (autovacuum, checkpoint_completion_target, data_checksums, idle_in_transaction_session_timeout, log_checkpoints, log_lock_waits, 
        #      log_min_duration_statement, log_temp_files, shared_preload_libraries, track_activity_query_size))

        msg = ''              
        if autovacuum != 'on':
            marker = MARK_WARN
            msg = "autovacuum is off.  "
        if checkpoint_completion_target <= 0.6:
            marker = MARK_WARN
            msg+= "checkpoint_completion_target is less than optimal.  "            
        if data_checksums != 'on':
            marker = MARK_WARN
            msg+= "data checksums is off.  "
        if idle_in_transaction_session_timeout == '0':
            marker = MARK_WARN
            msg+= "idle_in_transaction_session_timeout is turned off.  "
        if log_checkpoints != 'on':
            marker = MARK_WARN
            msg+= "log_checkpoints is off.  "
        if log_lock_waits != 'on':
            marker = MARK_WARN
            msg+= "log_lock_waits is off.  "            
        if log_min_duration_statement == '-1':
            marker = MARK_WARN
            msg+= "log_min_duration_statement is off.  "
        if log_temp_files == '-1':
            marker = MARK_WARN
            msg+= "log_temp_files is off.  "             
        if 'pg_stat_statements' not in shared_preload_libraries:
            marker = MARK_WARN
            msg+= "pg_stat_statements extension is not loaded.  "
        if track_activity_query_size < 8192:
            marker = MARK_WARN
            msg+= "track_activity_query_size may need to be increased or log queries may be truncated.  " 

        if msg != '':
            marker = MARK_WARN        
        else:
            marker = MARK_OK
            msg = "No configuration problems detected."

        print (marker+msg)

        
        ############################################################
        # Check checkpoints, background writers, and backend writers
        ############################################################
        # v2.1 fix: divident could be zero and cause division by zero error, so check first.
        sql = "select buffers_checkpoint + buffers_checkpoint + buffers_clean + buffers_backend as buffers from pg_stat_bgwriter"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get background/backend buffers count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors        

        if int(results) == 0:
            marker = MARK_WARN
            msg = "No buffers to check for checkpoint, background, or backend writers."
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10004;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint/Background/Backend Writers</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
            print (marker+msg)
        else:            
            sql = "select checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, checkpoint_write_time / 1000 as checkpoint_write_time, checkpoint_sync_time / 1000 as checkpoint_sync_time, (100 * checkpoints_req) / (checkpoints_timed + checkpoints_req) AS checkpoints_req_pct,    pg_size_pretty(buffers_checkpoint * block_size / (checkpoints_timed + checkpoints_req)) AS avg_checkpoint_write,  pg_size_pretty(block_size * (buffers_checkpoint + buffers_clean + buffers_backend)) AS total_written,  100 * buffers_checkpoint / (buffers_checkpoint + buffers_clean + buffers_backend) AS checkpoint_write_pct,    100 * buffers_clean / (buffers_checkpoint + buffers_clean + buffers_backend) AS background_write_pct, 100 * buffers_backend / (buffers_checkpoint + buffers_clean + buffers_backend) AS backend_write_pct from pg_stat_bgwriter, (SELECT cast(current_setting('block_size') AS integer) AS block_size) bs"

            cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get background/backend writers: %d %s\nsql=%s\n" % (rc, results, sql)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            cols = results.split('|')
            checkpoints_timed     = int(cols[0].strip())
            checkpoints_req       = int(cols[1].strip())
            buffers_checkpoint    = int(cols[2].strip())
            buffers_clean         = int(cols[3].strip())
            maxwritten_clean      = int(cols[4].strip())
            buffers_backend       = int(cols[5].strip())
            buffers_backend_fsync = int(cols[6].strip())
            buffers_alloc         = int(cols[7].strip())
            checkpoint_write_time = int(float(cols[8].strip()))
            checkpoint_sync_time  = int(float(cols[9].strip()))
            checkpoints_req_pct   = int(cols[10].strip())
            avg_checkpoint_write  = cols[11].strip()
            total_written         = cols[12].strip()
            checkpoint_write_pct  = int(cols[13].strip())
            background_write_pct  = int(cols[14].strip())
            backend_write_pct     = int(cols[15].strip())

            # calculate average checkpoint time
            avg_checkpoint_seconds = ((checkpoint_write_time + checkpoint_sync_time) / (checkpoints_timed + checkpoints_req))

            if self.verbose:
                msg = "chkpt_time=%d chkpt_req=%d  buff_chkpt=%d  buff_clean=%d  maxwritten_clean=%d  buff_backend=%d  buff_backend_fsync=%d  buff_alloc=%d, chkpt_req_pct=%d avg_chkpnt_write=%s total_written=%s chkpnt_write_pct=%d background_write_pct=%d  backend_write_pct=%d avg_checkpoint_time=%d seconds" \
                % (checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, checkpoints_req_pct, avg_checkpoint_write, total_written, checkpoint_write_pct, background_write_pct, backend_write_pct, avg_checkpoint_seconds)
                print (msg)

            msg = ''
            marker = MARK_OK
            if buffers_backend_fsync > 0:
                marker = MARK_WARN
                msg = "bgwriter fsync request queue is full. Backend using fsync.  "
            if backend_write_pct > (checkpoint_write_pct + background_write_pct):
                marker = MARK_WARN
                msg += "backend writer doing most of the work.  Consider decreasing \"bgwriter_delay\" by 50% or more to make background writer do more of the work.  "
            if maxwritten_clean > 500000:
                # for now just use a hard coded value of 500K til we understand the math about this better
                marker = MARK_WARN
                msg += "background writer stopped cleaning scan %d times because it had written too many buffers.  Consider increasing \"bgwriter_lru_maxpages\".  " % maxwritten_clean
            if checkpoints_req > checkpoints_timed:
                marker = MARK_WARN
                msg += "\"checkpoints requested\" contributing to a lot more checkpoints (%d) than \"checkpoint timeout\" (%d).  Consider increasing \"checkpoint_segments or max_wal_size\".  " % (checkpoints_req, checkpoints_timed)            
            if buffers_backend_fsync > 0:
                marker = MARK_WARN
                msg += "storage problem since fsync queue is completely filled. buffers_backend_fsync = %d." % (buffers_backend_fsync)            
            if buffers_clean > buffers_backend:
                marker = MARK_WARN
                msg += "backends doing most of the cleaning. Consider increasing bgwriter_lru_multiplier and decreasing bgwriter_delay.  It could also be a problem with shared_buffers not being big enough."                        
           
            if marker == MARK_OK:
                msg = "No problems detected with checkpoint, background, or backend writers."

            print (marker+msg)


        ########################
        # orphaned large objects
        ########################

        if self.in_recovery:
            #NOTE: cannot run this against slaves since vacuumlo will attempt to create temp table
            numobjects = "-1"
        else:
            # v1.2 fix: always use provided port number
            if self.dbuser == '':
                user_clause = "-p %s" % (self.dbport)
            elif self.dbhost == '':
                user_clause = " %s -U %s -p %s" % (self.dbuser, self.dbport)
            else:
                user_clause = " -h %s -U %s -p %s" % (self.dbhost, self.dbuser, self.dbport)

            cmd = "%s/vacuumlo -n %s %s" % (self.pgbindir, user_clause, self.database)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get orphaned large objects: %d %s\ncmd=%s\n" % (rc, results, cmd)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors

            # expecting substring like this --> "Would remove 35 large objects from database "agmednet.core.image"."
            numobjects = (results.split("Would remove"))[1].split("large objects")[0]

        if int(numobjects) == -1:
            marker = MARK_OK
            msg = "N/A: Unable to detect orphaned large objects on slaves."
        elif int(numobjects) == 0:
            marker = MARK_OK
            msg = "No orphaned large objects were found."
        else:
            marker = MARK_WARN
            msg = "%d orphaned large objects were found.  Consider running vacuumlo to remove them." % int(numobjects)

        print (marker+msg)

        ##################################
        # Check for bloated tables/indexes
        ##################################
        sql = "SELECT count(*) FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get table/index bloat count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.bloatedtables = False
            msg = "No bloated tables/indexes were found."
        else:
            marker = MARK_WARN
            self.bloatedtables = True
            msg = "%d bloated tables/indexes were found." % int(results)

        print (marker+msg)


        ##########################
        # Check for unused indexes
        ##########################
        sql="SELECT count(*) FROM pg_stat_user_indexes JOIN pg_index USING(indexrelid) WHERE idx_scan = 0 AND idx_tup_read = 0 AND idx_tup_fetch = 0 AND NOT indisprimary AND NOT indisunique AND NOT indisexclusion AND indisvalid AND indisready AND pg_relation_size(indexrelid) > 8192"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get unused indexes count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.unusedindexes = False
            msg = "No unused indexes were found."
        else:
            marker = MARK_WARN
            self.unusedindexes = True
            msg = "%d unused indexes were found." % int(results)

        print (marker+msg)
        
        ###################################
        # Check for short-lived connections
        ###################################
        sql="select cast(extract(epoch from avg(now()-backend_start)) as integer) as age from pg_stat_activity"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get average connection time: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        avgsecs = int(results)
        if avgsecs > 172800:
            # 24 hours, so warn to refresh connections
            marker = MARK_WARN
            msg = "Connections average more than 24 hours (%d minutes). Consider refreshing these connections 2-3 times per day." % (avgsecs / 60)
        elif avgsecs >= 120:
            marker = MARK_OK
            msg = "Connections average more than 2 minutes (%d). This seems acceptable." % (avgsecs / 60)
        elif avgsecs < 200:
            marker = MARK_WARN
            msg = "Connections average less than 2 minutes (%d).  Use or tune a connection pooler to keep these connections alive longer." % (avgsecs / 60)
        print (marker+msg)        


        ####################################
        # Check for vacuum freeze candidates
        ####################################
        sql="WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select count(c.*) from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 50"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get vacuum freeze candidate count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.freezecandidates = False
            msg = "No vacuum freeze candidates were found."
        else:
            marker = MARK_WARN
            self.freezecandidates = True
            msg = "%d vacuum freeze candidates were found." % int(results)
        print (marker+msg)


        ##############################
        # Check for analyze candidates
        ##############################
        sql="select count(*) from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = t.schemaname and t.tablename = c.relname and t.schemaname = u.schemaname and t.tablename = u.relname and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples > 0 and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null and last_analyze is null and last_autoanalyze is null ) or (now()::date  - last_vacuum::date > 60 AND now()::date - last_autovacuum::date > 60 AND now()::date  - last_analyze::date > 60 AND now()::date  - last_autoanalyze::date > 60)))"
        cmd = "psql %s -At -X -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get vacuum analyze candidate count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.analyzecandidates = False
            msg = "No vacuum analyze candidates were found."
        else:
            marker = MARK_WARN
            self.analyzecandidates = True
            msg = "%d vacuum analyze candidate(s) were found." % int(results)
        print (marker+msg)

        #############################
        ### Check for directory sizes
        #############################
        # assume we already got datadir from show variable earlier
        #rc, results = self.get_datadir()
        #if rc != SUCCESS:
        #    print ("Unable to get directory sizes.")
        # get sizes for datadir and pg_wal.
        # for now treat pg_wal as being under the same mount point as datadir
        #select sum(size) from pg_ls_waldir()
        cmd = "df -h %s | tail -n1 | awk '{print($5)}' | cut -d'%%' -f1" % self.datadir
        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            print ("Unable to get directory sizes.")
        else:
            #print ("df -h results = %s" % results)
            pctused = int(results)
            if pctused > 75:
                marker = MARK_WARN
                msg = "Data Directory Usage is high: %d%% used" % pctused
            else:    
                marker = MARK_OK
                msg = "Data Directory Usage is acceptable: %d%% used" % pctused                
        print (marker+msg)
        

        return SUCCESS, ""


    ###########################################################
    def delay(self, freeze):
        return SUCCESS, ""
       

##### END OF CLASS DEFINITION

#############################################################################################
def setupOptionParser():
    parser = OptionParser(add_help_option=False, description=DESCRIPTION)
    parser.add_option("-h", "--dbhost",         dest="dbhost",   help="DB Host Name or IP",                     default="",metavar="DBHOST")
    parser.add_option("-p", "--port",           dest="dbport",   help="db host port",                           default="5432",metavar="DBPORT")
    parser.add_option("-U", "--dbuser",         dest="dbuser",   help="db host user",                           default="",metavar="DBUSER")
    parser.add_option("-d", "--database",       dest="database", help="database name",                          default="",metavar="DATABASE")
    parser.add_option("-n", "--schema",         dest="schema", help="schema name",                              default="",metavar="SCHEMA")
    parser.add_option("-g", "--genchecks",      dest="genchecks", help="do default checks",                     default=False, action="store_true")
    parser.add_option("-w", "--waitslocks",     dest="waitslocks", type=int, help="waits, locks",               default=-999,metavar="WAITSLOCKS")
    parser.add_option("-l", "--longquerymins",  dest="longquerymins", type=int, help="long query mins",         default=-999,metavar="LONGQUERYMINS")
    parser.add_option("-c", "--cpus",           dest="cpus", type=int, help="cpus available",                   default=-999,metavar="CPUS")
    parser.add_option("-i", "--idleintransmins",dest="idleintransmins", type=int, help="idle in trans mins",    default=-999,metavar="IDLEINTRANSMINS")
    parser.add_option("-o", "--idleconnmins",   dest="idleconnmins", type=int, help="idle connections mins",    default=-999,metavar="IDLECONNMINS")
    parser.add_option("-e", "--environment",    dest="environment", help="environment identifier",              default="",metavar="ENVIRONMENT")    
    parser.add_option("-t", "--testmode",       dest="testmode", help="testing email addr",                     default=False, action="store_true")
    parser.add_option("-v", "--verbose",        dest="verbose", help="Verbose Output",                          default=False, action="store_true")
    parser.add_option("-s", "--slacknotify",    dest="slacknotify", help="Slack Notifications",                 default=False, action="store_true")
    parser.add_option("-m", "--mailnotify",    dest="mailnotify", help="Mail Notifications",                    default=False, action="store_true")

    return parser

#############################################################################################

#################################################################
#################### MAIN ENTRY POINT ###########################
#############################################@###################

optionParser   = setupOptionParser()
(options,args) = optionParser.parse_args()

# load the instance
pg = maint()

# Load and validate parameters
rc, errors = pg.set_dbinfo(options.dbhost, options.dbport, options.dbuser, options.database, options.schema, \
                           options.genchecks, options.waitslocks, options.longquerymins, options.idleintransmins, \
                           options.idleconnmins,  options.cpus, options.environment, options.testmode, options.verbose, options.slacknotify, options.mailnotify, sys.argv)
if rc != SUCCESS:
    print (errors)
    pg.cleanup()
    optionParser.print_help()
    sys.exit(1)

print ("%s  version: %.1f  %s     Python Version: %d     PG Version: %s  local detected=%r   PG Database: %s\n\n" \
       % (PROGNAME, VERSION, ADATE, sys.version_info[0], pg.pgversionminor, pg.local, pg.database))

#print ("globals=%s" % globals())
#print ("locals=%s" % locals())

rc, results = pg.do_report()
if rc < SUCCESS:
    pg.cleanup()
    sys.exit(1)

pg.cleanup()

sys.exit(0)
