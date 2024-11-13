#!/usr/bin/python

import sys
import subprocess

def runBlaunch(argv):
    args = argv
    args[0] = 'srun'
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout,stderr = proc.communicate()

runBlaunch(sys.argv)
