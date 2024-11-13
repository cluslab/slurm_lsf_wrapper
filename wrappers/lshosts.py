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

host_info = {}
global flag 

def checkOpts(argv):
    global flag
    flag = 0
    for e in argv[1:]:
        if e == '-h':
            doHelp()
            sys.exit(0)
        elif e == '-V':
            getSlurmVersion()
            sys.exit(0)
        elif e == '-T':
            flag = 1
            getHostInfo()
            getSocketsCores()
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
                    print(e + ": unknown host name.")
                    sys.exit(0)
                else:
                    hlist.append(e)

def doHelp():
    print("Usage:")
    print("    lshosts [options] ")
    print("Options:")
    print("    -h ")
    print("        Prints command usage to stderr and exits.\n")
    print("    -T ")
    print("        Displays host topology information for each host or cluster.\n")
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


def getSocketsCores():
    global host_info
    for HOSTNAME in host_info.keys():
        print("Host[" + host_info[HOSTNAME][0] + "] " + HOSTNAME)
        k = 0
        for i in range(0, int(host_info[HOSTNAME][1])):
            print("    Socket")
            for j in range(0, int(host_info[HOSTNAME][2])):
                print("        core(" + str(k) + ")")
                k += 1
        print("\n")    

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


def getControlMachine():
    cmdstr = 'scontrol show config | grep SlurmctldHost'
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    match = re.search(r"=\s+(\w+)", stdout)
    control_host = None
    if match:
        control_host = match.group(1)
    return control_host
def getHostInfo():
    global hdict, headPrint, hlist, host_info, flag
    cmdstr = 'scontrol show nodes | grep "NodeName\|mem\|CPU\|Sockets"'
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    parts = [lines[i:i+4] for i in range(0, len(lines)-2, 4)]
    for line in parts:
        kv = {}
        if line == '':
            continue
        for item in line:
            item = item.split()
            result = dict(e.split('=',1) for e in item)
            kv.update(result)
        HOSTNAME = kv['NodeName']  
        CONTROLMACHINE = getControlMachine()
        if len(hlist) != 0:
            if HOSTNAME not in hlist:
                continue
        if HOSTNAME == CONTROLMACHINE:
            RESOURCES = "(mg)"
        else:
            RESOURCES = "()"
        TYPE = kv['Arch']
        NCPUS = kv['CPUTot']
        tres_parts = kv['CfgTRES'].split(',')
        mem_info = [part for part in tres_parts if part.startswith('mem=')]
        if mem_info:
            mem_value = mem_info[0].split('=')[1]
            MAXMEM = mem_value
        else:
            MAXMEM = ''
        NUM_SOCKETS = kv['Sockets']  
        NUM_CORESPERSOCKET = kv['CoresPerSocket']
        
        host_info[HOSTNAME] = (MAXMEM, NUM_SOCKETS, NUM_CORESPERSOCKET)
        if flag == 1:
            continue
        
        if headPrint == False:
            print("HOST_NAME     type      model    cpuf     ncpus     maxmem     maxswp   server   RESOURCES")
            headPrint = True
        print(HOSTNAME.ljust(13) + TYPE.ljust(9) + "Unknown".ljust(12) + "1.0".ljust(10) + NCPUS.ljust(8) + MAXMEM.ljust(13) + "-".ljust(10) + "Yes".ljust(8) + RESOURCES)

checkOpts(sys.argv)  
getHostInfo()
