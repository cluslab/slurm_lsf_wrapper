#!/usr/bin/python
import datetime
import time
import sys

import subprocess

debug = False

opt_r = False
opt_d = False
opt_a = False
opt_u = ''
opt_G = ''
opt_J = ''
opt_p = False
opt_s = False
opt_no = True  #without any state options as: -p -d -a -s -r
jobids = []

headPrint = False
notfoundjobids = []

def checkOpts(argv):
    global opt_r, opt_d, opt_a, opt_u, opt_G, opt_J, opt_p, opt_s, opt_no, jobids
    fill_u = False
    fill_G = False
    fill_J = False
    isJobId = False
    for e in argv[1:]:
        if fill_u:
            fill_u = False
            opt_u = e
        elif fill_G:
            fill_G = False
            opt_G = e
        elif fill_J:
            fill_J = False
            opt_J = e
        elif isJobId:
            if not e.isdigit():
                print(e+": Illegal job ID.")
                sys.exit(0)
            else:
                jobids.append(e)
        elif e == '-h':
            doHelp()
            sys.exit(0)
        elif e == '-r':
            opt_r = True
            opt_no = False
        elif e == '-d':
            opt_d = True
            opt_no = False
        elif e == '-a':
            opt_a = True
            opt_no = False
        elif e == '-p':
            opt_p = True
            opt_no = False
        elif e == '-s':
            opt_s = True
            opt_no = False
        elif e == '-u':
            fill_u = True
            if opt_G != '':
                doHelp()
                sys.exit(0)
        elif e == '-G':
            fill_G = True
            if opt_u != '':
                doHelp()
                sys.exit(0)
        elif e == '-J':
            fill_J = True
        else:
            if e[0] == '-':
                sys.stderr.write("bjobs: illegal option -- "+e[1:]+'\n')
                doHelp()
                sys.exit(0)
            else:
                isJobId = True
                if not e.isdigit():
                    sys.stderr.write(e+": Illegal job ID.\n")
                    sys.exit(0)
                else:
                    jobids.append(e)
    if fill_u:
        sys.stderr.write("bjobs: illegal option -- u\n")
        sys.exit(0)
    if fill_G:
        sys.stderr.write("bjobs: illegal option -- G\n")
        sys.exit(0)
    if fill_J:
        sys.stderr.write("bjobs: illegal option -- J\n")
        sys.exit(0)

def doHelp():
    print("Usage:")
    print("    bjobs [options] [jobId ...]")
    print("Options:")
    print("    -h ")
    print("        brief help message\n")
    print("    -r ")
    print("        display running jobs\n")
    print("    -d ")
    print("        display finished jobs\n")
    print("    -p ")
    print("        display pending jobs\n")
    print("    -s ")
    print("        display suspend jobs\n")
    print("    -a ")
    print("        display all jobs in the system cache\n")
    print("    -u <user_name>  ")
    print("        display specific user's jobs, conflict with -G\n")
    print("    -G <user_group_name> ")
    print("        display specific user group's jobs, conflict with -u\n")
    print("    -J <job_name> ")
    print("        display jobs with specfic job name\n")

def dispRunJob():
    if opt_no or opt_a or opt_r:
        return True
    return False
def dispPendJob():
    if opt_p or opt_a or opt_no:
        return True
    return False
def dispFinishJob():
    if opt_d or opt_a:
        return True
    return False
def dispExitJob():
    if opt_d or opt_a:
        return True
    return False
def dispSuspJob():
    if opt_s or opt_a or opt_no:
        return True
    return False
def notFoundMessage():
    if opt_d:
        sys.stderr.write("No DONE/EXIT job found\n")
    elif opt_r:
        sys.stderr.write("No running job found\n")
    elif opt_s:
        sys.stderr.write("No suspended job found\n")
    elif opt_p:
        sys.stderr.write("No pending job found\n")
    elif opt_no:
        sys.stderr.write("No unfinished job found\n")
    elif opt_a:
        sys.stderr.write("No job found\n")

def getPrintJob(jobid):
    global headPrint
    cmdstr = "scontrol show job"
    #if jobid.isdigit():
    #    cmdstr = cmdstr +" "+  jobid
    #    cmdstr += " -d"
    #    print(cmdstr)
    cmdstr = cmdstr +" "+  jobid
    cmdstr += " -d"
    if debug:
        print cmdstr
        
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = proc.communicate()[0]
    if debug:
        print output
    lines = output.split('\n\n')
    if debug:
        print lines
    for line in lines:
        if 'JobId' not in line:
            if line != '' and jobid.isdigit():
                notfoundjobids.append(jobid)
            break
        item = line.split()
        kv = dict(e.split('=',1) for e in item)
        JOBID = kv['JobId'].ljust(8)
        USER = kv['UserId'].split("(")[0]
        if opt_u != '' and opt_u != USER:
            continue
        USER = USER.ljust(8)
        USER_GROUP = kv['GroupId'].split("(")[0]
        if opt_G != '' and opt_G != USER_GROUP:
            continue
        STAT = kv['JobState']
        if STAT == "RUNNING":
            if dispRunJob() or jobid != '':
                STAT = "RUN"
            else:
                continue
        elif STAT == "PENDING":
            if dispPendJob() or jobid != '':
                STAT = "PEND"
            else:
                continue
        elif STAT == "SUSPENDED":
            if dispSuspJob() or jobid != '':
                STAT = "SUSP"
            else:
                continue
        elif STAT == "COMPLETED":
            if dispFinishJob() or jobid != '':
                STAT = "DONE"
            else:
                continue
        elif STAT == "CANCELLED" or STAT == "FAILED":
            if dispExitJob() or jobid != '':
                STAT = "EXIT"
            else:
                continue
        else:
            STAT = "UNKWN"
        STAT = STAT.ljust(6)
        QUEUE = kv['Partition'].ljust(11)
        FROM_HOST = kv['AllocNode:Sid'].split(":")[0].ljust(12) #AllocNode
        CPUS_TASK = kv["CPUs/Task"]
        EXEC_HOST = []
        node_start_index = line.find("JOB_GRES=")
        node_end_index = line.find("MinCPUsNode=")
        if node_start_index != -1:
            flag = 1
            nodes_data = line[node_start_index:node_end_index].strip()
            node_pairs = nodes_data.split("Nodes=")[1:]
            nodes_info = {}
            
            for pair in node_pairs:
                nodes = pair.split("CPU_IDs=")[0].strip()
                cpu_ids = pair.split("CPU_IDs=")[1].split()[0]
                nodes_info[nodes] = {"Nodes": nodes ,"CPU_IDs": cpu_ids,}
            for nodes, info in nodes_info.items():
                cpu_ids = info["CPU_IDs"]
                cpu_ids_split = cpu_ids.split("-")
                start_id = int(cpu_ids_split[0])
                end_id = int(cpu_ids_split[-1])
                cpu_count = end_id - start_id + 1
                num_node = int(cpu_count) / int(CPUS_TASK)
                EXEC_HOST.extend([nodes.ljust(12)] * num_node)
        else:
            flag = 0        
        JOB_NAME = kv['JobName']
        if opt_J != '' and opt_J != JOB_NAME:
            continue
        JOB_NAME = JOB_NAME.ljust(11)             
        SUBMIT_TIME = kv['SubmitTime']
        subtime = datetime.datetime.strptime(SUBMIT_TIME,"%Y-%m-%dT%H:%M:%S")
        SUBMIT_TIME = subtime.strftime("%b %d %H:%M")
        if headPrint == False:
            print("JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME")
            length = len(JOBID+ USER+ STAT+QUEUE+FROM_HOST)
            headPrint = True
        if flag == 1:
            for i, exec_host in enumerate(EXEC_HOST):
                if i == 0:
                    print(JOBID+ USER+ STAT+QUEUE+FROM_HOST+EXEC_HOST[0]+JOB_NAME+SUBMIT_TIME)
                else:
                    print(" " * length + exec_host)
        else:
            EXEC_HOST = kv['NodeList'].ljust(12)
            print(JOBID+ USER+ STAT+QUEUE+FROM_HOST+EXEC_HOST+JOB_NAME+SUBMIT_TIME)
    if headPrint == False and jobid == '':
        notFoundMessage()


checkOpts(sys.argv)
for jobid in jobids:
    getPrintJob(jobid)
if not jobids:
    getPrintJob('')
for njobid in notfoundjobids:
    sys.stderr.write("Job <"+njobid+"> is not found\n")

