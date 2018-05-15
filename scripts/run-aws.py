#! /usr/bin/env python

## include packages
import commands
import subprocess # execute commands
import sys    # parse user input
import signal # bind interrupt handler

import time #sleep

import xml.etree.cElementTree as ET ## parsing the xml file

import threading #lock

import os  # change dir

#import zmq # network communications, if necessary

## user defined packages
from run_util import print_with_tag
from run_util import change_to_parent

#====================================#

original_sigint_handler = signal.getsignal(signal.SIGINT)

## config parameters and global parameters
## default mac set
mac_set = ["10.0.0.100", "10.0.0.101", "10.0.0.102", "10.0.0.103", "10.0.0.104", "10.0.0.105"]

PORT = 8090
#port = 9080
## benchmark constants
BASE_CMD = "./%s --bench %s --txn-flags 1  --verbose --config %s"
output_cmd = "1>/dev/null 2>&1 &" ## this will ignore all the output
OUTPUT_CMD_LOG = " 1>log 2>&1 &" ## this will flush the log to a file


## bench config parameters

base_cmd = ""
config_file = "config.xml"

## start parese input parameter"
program = "dbtest"

exe = ""
bench = "bank"
arg = ""

ssh_base = ["ssh","-i", "../aws/tp.pem","-o","StrictHostKeyChecking=no","-n","-f"]

## lock
int_lock = threading.Lock()

# ending flag
script_end = False

#====================================#
## class definations
class GetOutOfLoop(Exception):
    ## a dummy class for exit outer loop
    pass

#====================================#
## helper functions
def copy_file(f):
    for host in mac_set:
        print_with_tag("copy","To %s" % host)
        subprocess.call(["scp", "-i","../aws/tp.pem","./%s" % f, "%s:%s" % (host,"~")])


def kill_servers(e):
    #  print "ending ... kill servers..."
    kill_cmd1 = "pkill %s --signal 2" % e
    # real kill
    kill_cmd2 = "sudo pkill %s" % e
    for i in xrange(len(mac_set)):
        subprocess.call( ssh_base + [mac_set[i], kill_cmd1])
        time.sleep(1)
        try:
            subprocess.call(ssh_base + [mac_set[i], kill_cmd2])
            bcmd = "ps aux | grep nocc"
            stdout, stderr = Popen(ssh_base + [m, bcmd],
                                   stdout=PIPE).communicate()
            assert(len(stdout.split("\n")) == 3)

        except:
            pass

## singal handler
def signal_int_handler(sig, frame):

    print_with_tag("ENDING", "End benchmarks")
    global script_end
    int_lock.acquire()

    if (script_end):
        int_lock.release()
        return

    script_end = True

    print_with_tag("ENDING", "send ending messages in SIGINT handler")
    print_with_tag("ENDING", "kill processes")
    signal.signal(signal.SIGINT, original_sigint_handler)
    kill_servers(exe)
    print_with_tag("ENDING", "kill processes done")
    time.sleep(1)
    int_lock.release()

    sys.exit(0)
    return

def parse_input():
    global config_file, exe,  base_cmd, arg, bench ## global declared parameters
    if (len(sys.argv)) > 1: ## config file name
        config_file = sys.argv[1]
    if (len(sys.argv)) > 2: ## exe file name
        exe = sys.argv[2]
    if (len(sys.argv)) > 3: ## program specified args
        args = sys.argv[3]
    if (len(sys.argv)) > 4:
        bench = sys.argv[4]
    base_cmd = (BASE_CMD % (exe, bench, config_file)) + " --id %d " + args
    return


# print "starting with config file  %s" % config_file
def parse_bench_parameters(f):
    global mac_set, mac_num
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "bench"

    mac_set = []
    for e in root.find("servers").find("mapping").findall("a"):
        mac_set.append(e.text.strip())
    return

def start_servers(macset, config, bcmd):
    assert(len(macset) >= 1)
    for i in xrange(1,len(macset)):
        cmd = (bcmd % (i)) + OUTPUT_CMD_LOG ## disable remote output
        subprocess.call(ssh_base + [macset[i],"rm *.log"]) ## clean remaining log
        subprocess.call(ssh_base + [macset[i], cmd])
    ## local process is executed right here
    ## cmd = "perf stat " + (bcmd % 0)
    cmd = bcmd % 0
    print cmd
    subprocess.call(cmd.split()) ## init local command for debug
    #subprocess.call(["ssh","-n","-f",macset[0],cmd])
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

    kill_servers(exe)
    prepare_files([exe,config_file])    ## copy related files to remote

    print "[START] cleaning remaining processes."

    time.sleep(1) ## ensure that all related processes are cleaned

    signal.signal(signal.SIGINT, signal_int_handler) ## register signal interrupt handler
    start_servers(mac_set, config_file, base_cmd) ## start server processes
    for i in xrange(10):
        ## forever loop
        time.sleep(10)
    signal_int_handler(0,1) ## 0,1 are dummy args
    return

#====================================#
## the code
if __name__ == "__main__":
    main()
