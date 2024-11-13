#!/usr/bin/python

import sys
import subprocess

opt_m = ''
opt_u = ''
qlist = []

class queueInfo:
    def __init__(self, name, status, prio):
        self.name = name
        self.status = status
        self.prio = prio
        self.PendJNum = 0
        self.SuspJNum = 0
        self.RunJNum = 0
    def setStatus(self, status):
        self.status = status
    def getStatus(self):
        return self.status
    def getQname(self):
        return self.name
    def getPrio(self):
        return self.prio
    def setPendJobNum(self, PendJNum):
        self.PendJNum = PendJNum
    def addPendJobNum(self):
        self.PendJNum += 1
    def getPendJobNum(self):
        return self.PendJNum
    def setSuspJobNum(self, SuspJNum):
        self.SuspJNum = SuspJNum
    def addSuspJobNum(self):
        self.SuspJNum += 1
    def getSuspJobNum(self):
        return self.SuspJNum
    def setRunJobNum(self, RunJNum):
        self.RunJNum = RunJNum
    def addRunJobNum(self):
        self.RunJNum += 1
    def getRunJobNum(self):
        return self.RunJNum 
    def getJobNum(self):
        return self.RunJNum + self.PendJNum + self.SuspJNum
    
qdict = {}

def doHelp():
    print("Usage:")
    print("    bqueues [options] [queue_names]")
    print("Options:")
    print("    -h ")
    print("        Brief help message\n")
    print("    -m <hostname>")
    print("        Displays the queues that can run jobs on the specified host\n")
    print("    -u <username>")
    print("        Displays the queues that can accept jobs from the specified user\n")

def checkOpts(argv):
    global opt_m,opt_u
    fill_m = False
    fill_u = False
    for e in argv[1:]:
        if fill_m:
            opt_m = e
            fill_m = False
        elif fill_u:
            opt_u = e
            fill_u = False
        elif e == '-m':
            fill_m = True
        elif e == '-u':
            fill_u = True
        elif e == '-h':
            doHelp()
            sys.exit(0)
        else:
            if e[0] == '-':
                print("bqueues: illegal option -- "+e[1:])
                doHelp()
                sys.exit(0)
            else:
                qlist.append(e)
    if fill_m:
        print("bqueues: option requires an argument -- m")
        doHelp()
        sys.exit(0)
    
def getFullPart(partName):
    cmdstr = "scontrol show part" + partName
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = proc.communicate()[0]
    lines = output.split('\n\n')
    partList = []
    for line in lines:
        if line == '' or 'PartitionName' not in line:
            continue
        item = line.split()
        kv = dict(e.split('=',1) for e in item)
        partList.append(kv)
    return partList

def getUserPart(user):
    kvs = getFullPart('')
    plist = []
    for kv in kvs:
        users = kv["AllowAccounts"].split(',')
        if user in users or "ALL" in users:
            plist.append(kv["PartitionName"])
    return plist

def getOpts():
    global qlist
    optstr = ''
    if opt_m != '':
        optstr += " -n " + opt_m
    if opt_u != '':
        plist = getUserPart(opt_u)
        plist.extend(qlist)
        qlist = list(set(plist))
    if qlist:
        optstr += " -p " + ','.join(qlist)
    return optstr

def getPartInfo():
    global qdict
    cmdstr = "sinfo -O partition,Available,PriorityTier" + getOpts()
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    for line in lines[1:]:
        if line == '':
            continue
        words = line.split()
        partname = words[0].strip()
        if partname[-1] == '*':
            partname = partname[:-1]
        status = words[1].strip()
        if status == 'up':
            status = 'Open:Active'
        elif status == 'down':
            status = 'Open:Inact'
        elif status == 'drain':
            status = 'Close:Active'
        elif status == 'inactive':
            status = 'Close:Inact'
        prio = words[2].strip()
        qdict[partname] = queueInfo(partname,status,prio)
    #for e in qdict.values():
    #    print(e.__dict__)

def getQueueInfo():
    global qdict
    cmdstr = "squeue -O partition,jobid,state"
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    lines = stdout.split('\n')
    for line in lines[1:]:
        if line == '':
            continue
        words = line.split()
        partname = words[0].strip()
        jobid = words[1].strip()
        state = words[2].strip()
        if qdict.has_key(partname):
            obj = qdict.get(partname)
            if state == "RUNNING":
                obj.addRunJobNum()
            elif state == "PENDING":
                obj.addPendJobNum()
            elif state == "SUSPENDED":
                obj.addSuspJobNum()
    #for e in qdict.values():
    #    print(e.__dict__)

def printQueue():
    header = "QUEUE_NAME      PRIO STATUS          MAX JL/U JL/P JL/H NJOBS  PEND   RUN  SUSP"
    if qdict:
        print(header)
    for key,queue in qdict.items():
        line = queue.getQname().ljust(16)
        line += queue.getPrio().center(4) + ' '
        line += queue.getStatus().ljust(13)
        line += '-'.rjust(6) + ' '
        line += '-'.rjust(4) + ' '
        line += '-'.rjust(4) + ' '
        line += '-'.rjust(4) + ' '
        line += str(queue.getJobNum()).rjust(5) + ' '
        line += str(queue.getPendJobNum()).rjust(5) + ' '
        line += str(queue.getRunJobNum()).rjust(5) + ' '
        line += str(queue.getSuspJobNum()).rjust(5) + ' '
        print(line)

checkOpts(sys.argv)
getPartInfo()
getQueueInfo()
printQueue()
