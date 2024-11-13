#!/usr/bin/python

import sys
import subprocess
import re

class jobmark:
    def __init__(self, jobid, exist):
        self.jobid = jobid
        self.exist = exist
    def getExist(self):
        return self.exist
    def getJobid(self):
        return self.jobid

jobids = ''
jobmarklist = []
opt_u = ''
opt_q = ''
opt_J = ''
opt_m = ''

argv_u = ''
argv_q = ''
argv_J = ''
argv_m = ''

def doHelp():
    print("Usage:")
    print("    bkill [options] [jobids]")
    print("Options:")
    print("    -h ")
    print("        Brief help message\n")
    print("    -u <username>")
    print("        Kill the jobs owned by the given user.\n")
    print("    -q <queue>")
    print("        Kill the jobs in the given queue.\n")
    print("    -J <job_name>")
    print("        Kill the jobs with the given name.\n")
    print("    -m <host_name>")
    print("        Kill the jobs on the host.\n")

def checkOpts(argv):
    global jobids, opt_u, opt_q, opt_J, opt_m, argv_J, argv_m, argv_u, argv_q
    fill_u = False
    fill_q = False
    fill_J = False
    fill_m = False
    for e in argv[1:]:
        if fill_u:
            opt_u = e
            fill_u = False
        elif fill_q:
            opt_q = e
            fill_q = False
        elif fill_J:
            opt_J = e
            fill_J = False
        elif fill_m:
            opt_m = e
            fill_m = False
        elif e == '-h':
            doHelp()
            sys.exit(0)
        elif e == '-u':
            fill_u = True
            argv_u = e
        elif e == '-q':
            fill_q = True
            argv_q = e
        elif e == '-J':
            fill_J = True
            argv_J = e
        elif e == '-m':
            fill_m = True
            argv_m = e
        else:
            if not e.isdigit():
                sys.stderr.write(e+": Illegal job ID.\n")
                sys.exit(0)
            joblist = getJobIds()
            if e not in joblist:
                jobmarklist.append(jobmark(e,False))
            else:
                jobmarklist.append(jobmark(e,True))
            jobids += ' '+e

    if  argv_u and fill_u:
        doHelp()
        sys.exit(0)
    elif argv_J and fill_J:
        doHelp()
        sys.exit(0)
    elif argv_q and fill_q:
        doHelp()
        sys.exit(0)
    elif argv_m and fill_m:
        doHelp()
        sys.exit(0)
      
def getJobIds(opts=''):
    queuelist = []
    cstr = "sinfo -O PARTITION"
    proc = subprocess.Popen(cstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cstrout,cstrerr = proc.communicate()
    lines = cstrout.split('\n')
    for e in lines[1:]:
        if e != '':
            if '*' not in e:
                queuelist.append(e.strip())
            else:
                e = e.replace('*', '')
                queuelist.append(e.strip())
                         
    joblist = []
    cmdstr = "squeue"+opts+" -O jobid"
    #print(cmdstr)
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    #print(stdout,stderr)

    match = re.search(r'\d+', stdout)
    if match:
        digits = match.group()
    else:
        digits = 0
    
    if opt_m and "Invalid node name" in stderr:
        sys.stderr.write(opt_m + ": Bad host name, host group name or cluster name\n")
        sys.exit(0)
    elif opt_m and len(stderr) == 0 and not digits:
        sys.stderr.write("No unfinished job found on host/group <" + opt_m + ">\n")
        sys.exit(0)
    elif opt_u and "Invalid user" in stderr:
        sys.stderr.write("No unfinished job found\n")
        sys.exit(0)
    elif opt_u and len(stderr) == 0 and not digits:
        sys.stderr.write("No unfinished job found\n")
        sys.exit(0)
    elif opt_J and len(stderr) == 0 and not digits:
        sys.stderr.write("Job <" + opt_J + "> is not found\n")
        sys.exit(0)
    elif opt_q and opt_q not in queuelist:
        sys.stderr.write(opt_q + ": No such queue\n")
        sys.exit(0)
    elif opt_q and opt_q in queuelist and len(stderr) == 0 and not digits:
        sys.stderr.write("No unfinished job in queue <"+ opt_q +">\n")
        sys.exit(0)
    
    lines = stdout.split('\n')
    for e in lines[1:]:
        if e != '':
            joblist.append(e.strip())
    return joblist

def markJoblist(joblist,flag=True):
    for e in joblist:
        jobmarklist.append(jobmark(e,flag))

def buildSubOpts():
    if jobmarklist:
        return ''
    sub_opts = ''
    ustr = '' if opt_u == '' else ' -u '+opt_u
    if ustr != '':
        joblist = getJobIds(ustr)
        markJoblist(joblist)
    sub_opts += ustr
    qstr = '' if opt_q == '' else ' -p '+opt_q
    if qstr != '':
        joblist = getJobIds(qstr)
        markJoblist(joblist)
    sub_opts += qstr
    Jstr = '' if opt_J == '' else ' -n '+opt_J
    if Jstr != '':
        joblist = getJobIds(Jstr)
        markJoblist(joblist)
    sub_opts += Jstr
    mstr = '' if opt_m == '' else ' -w '+opt_m
    if mstr != '':
        joblist = getJobIds(mstr)
        markJoblist(joblist)
    sub_opts += mstr
    return sub_opts

def doKill():
    sub_opts = buildSubOpts()
    cmdstr = "scancel "+sub_opts + jobids
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    for e in jobmarklist:
        if e.getExist():
            print("Job <"+e.getJobid()+"> is being terminated")
        else:
            sys.stderr.write("Job <"+e.getJobid()+">: No matching job found\n")

checkOpts(sys.argv)
doKill()
