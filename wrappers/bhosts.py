#!/usr/bin/python
import datetime
import time
import sys
import re

import subprocess
headPrint = False
hlist = []
Exist_list = []
exist_list = []

def checkOpts(argv):
    for e in argv[1:]:
        if e == '-h':
            doHelp()
            sys.exit(0)
        else:
            if e[0] == '-':
                print("bhosts: illegal option -- "+e[1:])
                doHelp()
                sys.exit(0)
            else:
                exist_list = getHostName()
                if e not in exist_list:
                    print(e + ": Bad host name, host group name or cluster name")
                    sys.exit(0)
                else:
                    hlist.append(e)

def doHelp():
    print("Usage:")
    print("    bhosts [options] ")
    print("Options:")
    print("    -h ")
    print("        Brief help message\n")
    print("    hostname ")
    print("        Displays only information about the specified hosts. \n")

def getHostName():
    cmdstr = "sinfo -h -O NodeHost "
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    for line in lines:
        if line != '':
            line = line.strip()
            Exist_list.append(line)
    return Exist_list

def getCoreJob(HOSTNAME,State):
    cmdstr = "squeue -w " + HOSTNAME + "-h -t " + State + ' -o "%C"'
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    cores = re.findall(r'\d+', stdout.decode())
    total_cores = sum(int(job) for job in cores)
    return str(total_cores)
    
def getHostInfo():
    global hdict, headPrint, hlist
    cmdstr = 'scontrol show nodes | grep -E "NodeHostName|CPU|State"'
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    parts = [lines[i:i+3] for i in range(0, len(lines)-1, 3)]
    for line in parts:
        kv = {}
        if line == '':
            continue
        for item in line:
            item = item.split()
            result = dict(e.split('=',1) for e in item)
            kv.update(result)
        HOSTNAME = kv['NodeHostName']  
        if len(hlist) != 0:
            if HOSTNAME not in hlist:
                continue
        HOSTNAME = HOSTNAME.ljust(20)
        STATE = kv['State']
        MAXCPU = kv['CPUTot'].ljust(7)
        
        if STATE == "IDLE" or STATE == "MIXED":
            STATE = "ok"
        elif STATE == "ALLOCATED":
            STATE = "closed"
        elif STATE == "DOWN":
            STATE = "unavail"
        else:
            STATE = "unreach"
        STATE = STATE.ljust(15)
        JL_U  = '-'.ljust(8)
        NJOBS = getCoreJob(HOSTNAME,"RUNNING,SUSPENDED").ljust(7)
        RUN   = getCoreJob(HOSTNAME,"RUNNING").ljust(7)
        SSUSP = "0".ljust(8)
        USUSP = getCoreJob(HOSTNAME,"SUSPENDED").ljust(8)
        RSV   = "0".ljust(8)
    
        if headPrint == False:
            print("HOST_NAME          STATUS       JL/U     MAX   NJOBS    RUN   SSUSP   USUSP    RSV")
            headPrint = True
        print(HOSTNAME + STATE + JL_U + MAXCPU + NJOBS + RUN + SSUSP + USUSP + RSV)

checkOpts(sys.argv)  
getHostInfo()
