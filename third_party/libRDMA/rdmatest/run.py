#! /usr/bin/env python

## include packages
import commands
import subprocess
import sys
import signal

import time
import xml.etree.cElementTree as ET ## parsing the xml file

from run_util import print_with_tag

#====================================#

## config parameters and global parameters
## default mac set
mac_set = [ ]

## bench config parameters
worker_num = 1;
scale_factor = worker_num;

nthread = 8
mac_num = 2

port = 8090
#port = 9080

config_file = "config.xml"

## start parese input parameter"
exe = ""
args = ""
base_cmd = "./%s %s"

#====================================#
## class definations
class GetOutOfLoop(Exception):
    ## a dummy class for exit outer loop
    pass

#====================================#
## helper functions
def copy_file(f):
    for host in mac_set:
        subprocess.call(["scp","./%s" % f,"%s:%s" % (host,"~")])


def kill_servers():
    #  print "ending ... kill servers..."
    kill_cmd = "pkill %s" % exe
    for i in xrange(len(mac_set)):
        subprocess.call(["ssh","-n","-f",mac_set[i],kill_cmd])
    return

## singal handler
def signal_int_handler(signal, frame):
    print_with_tag("ENDING","send ending messages in SIGINT handler")
    print_with_tag("ENDING","kill processes")
    kill_servers()
    sys.exit(0)
    return

def parse_input():
    global config_file, exe, args, base_cmd
    if (len(sys.argv)) > 1: ## config file name
        config_file = sys.argv[1]
    if (len(sys.argv)) > 2: ## exe
        exe = sys.argv[2]
    if (len(sys.argv)) > 3: ## cmdline arg to exe
        args = sys.argv[3]
    base_cmd = (base_cmd % (exe, args)) + " -n %d" + " 1>log 2>&1 &"
    return


# print "starting with config file  %s" % config_file
def parse_bench_parameters(f):
    global mac_set, mac_num
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "param"

    mac_set = []
    for e in root.find("network").findall("a"):
        mac_set.append(e.text.strip())
    return

def start_servers(macset,config,bcmd):
    assert(len(macset) >= 1)
    for i in xrange(1,len(macset)):
        cmd =  bcmd % (i)
        print cmd
        subprocess.call(["ssh","-n","-f",macset[i],cmd])
    ## local process is executed right here
    cmd = bcmd % 0
    subprocess.call(cmd.split())
    return

def prepare_files(files):
    print "Start preparing start file"
    for f in files:
        copy_file(f)
    return


#====================================#
## main function
def main():
    global base_cmd
    parse_input() ## parse input from command line
    parse_bench_parameters(config_file) ## parse bench parameter from config file
    print "[START] Input parsing done."

    kill_servers()
    prepare_files([exe,config_file])    ## copy related files to remote

    print "[START] cleaning remaining processes."

    time.sleep(1) ## ensure that all related processes are cleaned

    signal.signal(signal.SIGINT, signal_int_handler) ## register signal interrupt handler
    start_servers(mac_set,config_file,base_cmd) ## start server processes
    while True:
        ## forever loop
        time.sleep(10)
    return

#====================================#
## the code
if __name__ == "__main__":
    main()
