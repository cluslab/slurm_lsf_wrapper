#!/usr/bin/python

import sys
import subprocess

opt_cmd = ''
opt_queue = ''
opt_jname = ''
opt_o = ''
opt_e = ''
opt_x = False
opt_cwd = ''
opt_n = ''
opt_W = ''
opt_m = ''
opt_I = ''

def doHelp():
    print("Usage:")
    print("    bsub [options] command [arguments]")
    print("Options:")
    print("    -h ")
    print("        Brief help message\n")
    print("    -q <queue>")
    print("        Specify the queue that this job will run on\n")
    print("    -J <job_name>")
    print("        Specify the name of this job\n")
    print("    -o <out_path>")
    print("        Specify the stdout output path for this job\n")
    print("    -e <error_path>")
    print("        Specify the stderr output path for this job\n")
    print("    -x ")
    print("        Run this job in exclusive mode.\n\
        Job will not share nodes with other jobs.\n")
    print("    -cwd <work_dir>")
    print("        Specify the working directory for this job\n")
    print("    -n <task_num>")
    print("        Specify task number of this job\n")
    print("    -W <time>")
    print("        Specify the runtime(minutes) limit of the job.\n")
    print("    -m <hostlist>")
    print("        Space separated list of hosts that this job will run on.\n")
    print("    -I ")
    print("        Submits an interactive job.")

def checkOpts(argv):
    global opt_cmd, opt_queue, opt_jname, opt_o, opt_e, opt_x
    global opt_cwd, opt_n, opt_W, opt_m, opt_I
    fill_queue = False
    fill_jname = False
    fill_o = False
    fill_e = False
    fill_cwd = False
    fill_n = False
    fill_W = False
    fill_m = False
    for e in argv[1:]:
        if fill_queue:
            fill_queue = False
            opt_queue = e
        elif fill_jname:
            fill_jname = False
            opt_jname = e
        elif fill_o:
            fill_o = False
            opt_o = e
        elif fill_e:
            fill_e = False
            opt_e = e
        elif fill_cwd:
            fill_cwd = False
            opt_cwd = e
        elif fill_n:
            if not e.isdigit():
                sys.stderr.write("Bad argument for option -n. Job not submitted.\n")
                sys.exit(0)
            fill_n = False
            opt_n = e
        elif fill_W:
            if not e.isdigit():
                sys.stderr.write(e+": Bad RUNLIMIT specification. Job not submitted.\n")
                sys.exit(0)
            fill_W = False
            opt_W = e
        elif fill_m:
            fill_m = False
            opt_m = e
        elif e == '-h':
            doHelp()
            sys.exit(0)
        elif e == '-q':
            fill_queue = True
        elif e == '-J':
            fill_jname = True
        elif e == '-o':
            fill_o = True
        elif e == '-e':
            fill_e = True
        elif e == '-x':
            opt_x = True
        elif e == '-I':
            opt_I = True
        elif e == '-cwd':
            fill_cwd = True
        elif e == '-n':
            fill_n = True
        elif e == '-W':
            fill_W = True
        elif e == '-m':
            fill_m = True
        else:
            if opt_cmd != '':
                opt_cmd += ' '
            else:
                if e[0] == '-':
                    sys.stderr.write("bsub: option cannot have an argument -- "+e[1:]+'\n')
                    doHelp()
                    sys.exit(0)
            opt_cmd += e
    if opt_cmd == '':
        sys.stderr.write("bsub: command required\n")
        sys.exit(0)

def getPart(partName):
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

def getDefaultPartName():
    kvs = getPart('')
    for kv in kvs:
        if kv['Default'] == 'YES':
            return kv['PartitionName']
    return ''

def checkErr(msg):
    if 'invalid partition' in msg:
        sys.stderr.write(opt_queue+": No such queue. Job not submitted.\n")
        sys.exit(0)
    elif 'Invalid node name' in msg:
        sys.stderr.write("bsub: Bad host name, host group name or cluster name. Job not submitted.\n")
    else:
        sys.stderr.write(msg)

def buildSubOpts():
    sub_opts = ''
    queuestr = '' if opt_queue == '' else ' -p '+opt_queue
    sub_opts += queuestr
    jnamestr = '' if opt_jname == '' else ' -J '+opt_jname
    sub_opts += jnamestr
    ostr = '' if opt_o == '' else ' -o '+opt_o
    sub_opts += ostr
    estr = '' if opt_e == '' else ' -e '+opt_e
    sub_opts += estr
    xstr = '' if opt_x == False else ' --exclusive'
    sub_opts += xstr
    cwdstr = '' if opt_cwd == '' else ' -D '+opt_cwd
    sub_opts += cwdstr
    nstr = '' if opt_n == '' else ' -n '+opt_n
    sub_opts += nstr
    Wstr = '' if opt_W == '' else ' -t '+opt_W
    sub_opts += Wstr
    mstr = str.replace(opt_m,' ',',')
    mstr = '' if mstr == '' else ' -w '+ mstr
    sub_opts += mstr
    return sub_opts
    
def subCmd(cmd):
    sub_opts = buildSubOpts()
    if opt_I:
        return optIsub(cmd, sub_opts)
    cmdstr = "sbatch"+sub_opts+" --wrap=\"" + cmd +"\""
    #print(cmdstr)
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    checkErr(stderr)
    l = stdout.split()
    if not l:
        sys.exit(0)
    jobid = l[-1]
    if opt_queue == '':
        defaultPart = getDefaultPartName()
        print("Job <"+jobid+"> is submitted to default queue <"+defaultPart+">.")
    else:
        print("Job <"+jobid+"> is submitted to queue <"+opt_queue+">.")

def optIsub(cmd, sub_opts):
    cmdstr = "srun"+sub_opts+" hostname;" + cmd
    proc = subprocess.Popen(cmdstr,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()
    #print(stdout,stderr)
    lines = stderr.split('\n')
    jobid = lines[0].split()[2]
    #print(stdout)
    lines = stdout.split('\n')
    hostname = lines[0]
    if opt_queue == '':
        defaultPart = getDefaultPartName()
        print("Job <"+jobid+"> is submitted to default queue <"+defaultPart+">.")
    else:
        print("Job <"+jobid+"> is submitted to queue <"+opt_queue+">.")
    print("<<Waiting for dispatch ...>>")
    print("<<Starting on "+hostname+">>")
    #print(lines)
    for e in lines[1:]:
        if e != '':
            print(e)
        

checkOpts(sys.argv)
subCmd(opt_cmd)
