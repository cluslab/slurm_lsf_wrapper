#!/usr/bin/python

import sys
import subprocess
import calendar

def getConfInfo():
    cmdstr = "scontrol show config | grep 'ClusterName\|Slurmctld(primary)'"
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = proc.communicate()[0]
    lines = output.split('\n')
    clusterName = lines[0].split("=",1)[1].strip()
    masterHost = lines[1].split()[2]
    return clusterName,masterHost

def getRelease():
    cmdstr = "sinfo -V"
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = proc.communicate()[0]
    return output.strip()

def release2Date(release):
    items = release.split()[1].split('.')
    year = "20"+items[0]
    month = calendar.month_abbr[int(items[1])]
    day = "1"
    return month + " " + day + " " + year
    
release = getRelease()
date = release2Date(release)
print(release+", "+date)
print("Copyright (C) 2010-2022 SchedMD LLC.")
print("")
cluster,master = getConfInfo()
print("My cluster name is "+ cluster)
print("My master name is "+ master)
