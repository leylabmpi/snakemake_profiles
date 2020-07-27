#!/usr/bin/env python3
import sys
import time
import re
import subprocess
from subprocess import Popen, PIPE, DEVNULL


#-- functions --#
def qstat_check(jobid, regex):
    p = Popen(['qstat'], stdout=PIPE)
    output, err = p.communicate()
    for x in output.decode().split('\n'):
        y = re.split(regex, x)
        if y[0] == jobid:
            if y[4] in ['r', 'qw', 't']:
                print('running')
            elif y[4] in ['Eqw', 'd']:
                print('failed')
            else:
                print('running')
            p.stdout.close()
            exit(0)

def acct_check(jobid, max_lines):
    acct_file = '/var/lib/gridengine/default/common/accounting'
    cmd = 'tail -n {max_lines} {acct_file} | tac | awk -F: -v id={jobid}'
    cmd = cmd.format(max_lines=max_lines, acct_file=acct_file, jobid=jobid)
    cmd += " '{if ($6 == id) {print $0; exit 0}}'"
    p = Popen([cmd], stdout=PIPE, shell=True)
    output, err = p.communicate()
    for i,x in enumerate(output.decode().split('\n')):
        if i > max_lines:
            p.stdout.close()
            break
        y = x.split(':')
        if len(y) < 12:
            continue
        else:
            if y[12] == '0':
                print('success')
            else:
                print('failed')
            p.stdout.close()
            exit(0)


def qacct_check(jobid):
    regex = re.compile(r' +')
    cmd = 'qacct -j {jobid}'.format(jobid=jobid)
    p = Popen([cmd], stdout=PIPE, shell=True, stderr=DEVNULL)
    output, err = p.communicate()
    for x in output.decode().split('\n'):
        x = regex.split(x)
        if x[0] == 'exit_status':
            if x[1] == '0':
                print('success')
            else:
                print('failed')            
            exit(0)
            
#-- main --#
jobid = sys.argv[1]
regex = re.compile(r' +')
try:
    # checking qstat
    qstat_check(jobid, regex)
    # if not listed via qstat, parsing sge accounting info
    acct_check(jobid, 1000)
    # if not listed at the end of the acct file, trying qacct
    qacct_check(jobid)
    print('running')
    
except KeyboardInterrupt:
    print("failed")


