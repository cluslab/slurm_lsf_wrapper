#!/usr/bin/python

import sys
import subprocess
from enum import Enum

class state(Enum):
    good = 1
    permission = 2
    notfound = 3

class jobmark:
    def __init__(self, jobid, state):
        self.jobid = jobid
        self.state = state
    def setState(self, state):
        self.state = state
    def getState(self):
        return self.state
    def getJobid(self):
        return self.jobid

jobids = ''
jobmarklist = []

def doHelp():
    print("Usage:")
    print("    bstop [options] [jobids]")
    print("Options:")
    print("    -h ")
    print("        Brief help message\n")
    print("    -u <username>")
    print("        Suspends only jobs owned by the specified user\n")
    print("    -m <hostname>")
    print("        Suspends only jobs dispatched to the specified host\n")
    print("    -J <job_name>")
    print("        Suspends only jobs with the specified name\n")
    print("    -q <queue>")
    print("        Suspends only jobs in the specified queue\n")
    print("    -a ")
    print("        Suspends all jobs\n")

def getJobIds(opt=''):
    global jobids,jobmarklist
    cmdstr = "squeue "+opt+" -O JOBID"
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    for line in lines[1:]:
        e = line.strip()
        if e.isdigit():
            jobids += " "+e
            jobmarklist.append(jobmark(e,state.good))

def checkOpts(argv):
    global jobids,jobmarklist
    fill_u = False
    fill_m = False
    fill_J = False
    fill_q = False
    for e in argv[1:]:
        if fill_u:
            getJobIds("-u "+e)
            break
        elif fill_m:
            getJobIds("-w "+e)
            break
        elif fill_J:
            getJobIds("-n "+e)
            break
        elif fill_q:
            getJobIds("-p "+e)
            break
        elif e == '-h':
            doHelp()
            sys.exit(0)
        elif e == '-u':
            fill_u = True
        elif e == '-m':
            fill_m = True
        elif e == '-J':
            fill_J = True
        elif e == '-q':
            fill_q = True
        elif e == '-a':
            getJobIds()
            break
        else:
            if not e.isdigit():
                sys.stderr.write(e+": Illegal job ID.\n")
                sys.exit(0)
            else:
                jobids += " "+e
                jobmarklist.append(jobmark(e,state.good))

def checkErr(errMsg):
    lines = errMsg.split('\n')
    for line in lines:
        words = line.split()
        if words:
            jobid = words[-1]
            for e in jobmarklist:
                if e.getJobid() == jobid:
                    if "permission" in words[0]:
                        e.setState(state.permission)
                    else:
                        e.setState(state.notfound)
        
def doStop():
    cmdstr = "scontrol suspend" + jobids
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    #print(stdout,stderr)
    checkErr(stderr)
    for e in jobmarklist:
        if e.getState() == state.good:
            print("Job <"+e.getJobid()+"> is being stopped") 
        elif e.getState() == state.permission:
            sys.stderr.write("Job <"+e.getJobid()+">: User permission denied\n")
        else:
            sys.stderr.write("Job <"+e.getJobid()+">: No matching job found\n")

checkOpts(sys.argv)
doStop()
