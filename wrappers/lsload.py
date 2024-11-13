#!/usr/bin/python
import datetime
import time
import sys
import re
import calendar
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
        elif e == '-V':
            getSlurmVersion()
            sys.exit(0)
        elif e == '-s':
            getResource()
            sys.exit(0)
        else:
            if e[0] == '-':
                print("bhosts: illegal option -- "+e[1:])
                doHelp()
                sys.exit(0)
            else:
                exist_list = getHostName()
                if e not in exist_list:
                    print("lsload: unknown host name " + e)
                    sys.exit(0)
                else:
                    hlist.append(e)

def doHelp():
    print("Usage:")
    print("    lsload [options] ")
    print("Options:")
    print("    -h ")
    print("        Prints command usage to stderr and exits.\n")
    print("    -s ")
    print("        Displays information about the specified resources. \n")
    print("    -V ")
    print("        Prints the Slurm release version to stderr and exits.\n")
    print("    hostname ")
    print("        Displays only information about the specified hosts. \n")

def getSlurmVersion():
    cmdstr = "srun -V "
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    items = stdout.split()[1].split('.')
    year = "20"+items[0]
    month = calendar.month_abbr[int(items[1])]
    day = "1"
    print(stdout.strip() + ", " + month + " " + day + " " + year)
    print("Copyright (C) 2010-2022 SchedMD LLC.")



def getResource():
    cmdstr = "sacctmgr show resource "
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    print("RESOURCE                                VALUE       LOCATION")
    if stdout:
        lines = stdout.decode('utf-8').split('\n')[2:]
        for line in lines:
            if line:
                elements = line.split()
                RESOURCE = elements[0]
                VALUE = elements[3]
                LOCATION = elements[1]
                print(RESOURCE.ljust(40) + VALUE.ljust(12) + LOCATION.ljust(8))

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

def getHostInfo():
    global hdict, headPrint, hlist, host_info, flag
    cmdstr = 'scontrol show nodes | grep "NodeName\|mem\|CPU\|State"'
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    parts = [lines[i:i+4] for i in range(0, len(lines)-1, 4)]
    for line in parts:
        kv = {}
        if line == '':
            continue
        for item in line:
            item = item.split()
            result = dict(e.split('=',1) for e in item)
            kv.update(result)
        HOSTNAME = kv['NodeName']  
        if len(hlist) != 0:
            if HOSTNAME not in hlist:
                continue 
        STATE = kv['State']
        if STATE == "IDLE" or STATE == "MIXED":
            STATE = "ok"
        elif STATE == "ALLOCATED":
            STATE = "busy"
        else:
            STATE = "unavail"
        CPULOAD = kv['CPULoad'].ljust(6)
        tres_parts = kv['CfgTRES'].split(',')
        mem_info = [part for part in tres_parts if part.startswith('mem=')]
        if mem_info:
            mem_value = mem_info[0].split('=')[1]
            MAXMEM = mem_value
        else:
            MAXMEM = ''
        
        if headPrint == False:
            print("HOST_NAME       status   r15s   r1m  r15m   ut    pg   ls    it   tmp   swp   mem")
            headPrint = True
        print(HOSTNAME.ljust(20) + STATE.ljust(5) + CPULOAD + CPULOAD + CPULOAD+ CPULOAD+ "0.0".ljust(6) + "0".ljust(6) + "0".ljust(6) + "0".ljust(6) + "-".ljust(5) + MAXMEM.ljust(8))

checkOpts(sys.argv)  
getHostInfo()
