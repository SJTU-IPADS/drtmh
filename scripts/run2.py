#! /usr/bin/env python

## include packages
import commands
import subprocess # execute commands
import sys    # parse user input
import signal # bind interrupt handler
import pickle
from runner import RoccRunner

import time #sleep

import xml.etree.cElementTree as ET ## parsing the xml file

import threading #lock

import os  # change dir

#import zmq # network communications, if necessary

## user defined packages
from run_util import print_with_tag
from run_util import change_to_parent

from runner import RoccRunner
from cmd import *

#====================================#

original_sigint_handler = signal.getsignal(signal.SIGINT)

## config parameters and global parameters
## default mac set
mac_set = ["10.0.0.100", "10.0.0.101", "10.0.0.102", "10.0.0.103", "10.0.0.104", "10.0.0.105"]
proxies = [] ## the proxies used to connect to remote servers
pwd     = "123"
mac_num = 1

PORT = 8090
#port = 9080
## benchmark constants
BASE_CMD = "./%s --bench %s --txn-flags 1  --verbose --config %s"
output_cmd = "1>/dev/null 2>&1 &" ## this will ignore all the output
OUTPUT_CMD_LOG = " 1>log 2>&1 &" ## this will flush the log to a file

FNULL = open(os.devnull, 'w')

## bench config parameters

base_cmd = ""
config_file = "config.xml"

## start parese input parameter"
program = "dbtest"

exe = ""
bench = "bank"
arg = ""

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
    global mac_num
    assert(mac_num <= len(mac_set))
    for i in xrange(mac_num):
        host = mac_set[i]
        #print_with_tag("copy","To %s" % host)
        subprocess.call(["scp", "./%s" % f, "%s:%s" % (host,"~")],stdout=FNULL,stderr=subprocess.STDOUT)

def kill_all(e,force=False):
    r = RoccRunner()
    kill_cmd2 = "pkill %s" % e

    killed_num = 0
    for i in xrange(mac_num):
        #print("kill at " + mac_set[i])
        if force:
            #print("force kill at " + mac_set[i])
            killed_num += 1
            execute_at(mac_set[i],kill_cmd2)
        else:
            if (r.check_liveness([],mac_set[i])):
                #print("check liveness success @" + mac_set[i])
                continue
            else:
                killed_num += 1
    if force:
        time.sleep(1)
        for i in xrange(mac_num):
            if (r.check_liveness([],mac_set[i])):
                print("[WARINING], kill failed at " + mac_set[i])
    return (killed_num == mac_num)

def kill(e):
    sigint = 2
    kill_cmd1 = "pkill %s --signal %d" % (e,sigint)

    for i in xrange(mac_num):
        slient_execute_at(mac_set[i],kill_cmd1)

    for i in xrange(3):
        print("kill try " + str(i))
        if kill_all(e):
            break
        time.sleep(0.5)
        if i == 2:
            kill_all(e,True)
    print("Kill done")


def kill_servers(e):
    #  print "ending ... kill servers..."
    sigint = 2
    kill_cmd1 = "pkill %s --signal %d" % (e,sigint)

    # real kill
    kill_cmd2 = "pkill %s" % e
    for i in xrange(mac_num):
        subprocess.call(["ssh", "-n","-f", mac_set[i], kill_cmd1])

    r = RoccRunner()

    for i in xrange(mac_num):
        c = 0
        while r.check_liveness([],mac_set[i]):
            time.sleep(2)
            c += 1
            if c > 5:
                break
        try:
            subprocess.call(["ssh", "-n","-f", mac_set[i], kill_cmd2])
            bcmd = "ps aux | grep nocc"
            stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=2",m, bcmd],
                                   stdout=PIPE).communicate()
            assert(len(stdout.split("\n")) == 3)

        except:
            pass
    return

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
    #kill_servers(exe)
    kill(exe)
    print_with_tag("ENDING", "kill processes done")
    time.sleep(1)
    int_lock.release()

    for p in proxies:
        p.close()
    sys.exit(0)
    return

def parse_input():
    global config_file, exe,  base_cmd, arg, bench, mac_num ## global declared parameters
    if (len(sys.argv)) > 1: ## config file name
        config_file = sys.argv[1]
    if (len(sys.argv)) > 2: ## exe file name
        exe = sys.argv[2]
    if (len(sys.argv)) > 3: ## program specified args
        args = sys.argv[3]
    if (len(sys.argv)) > 4:
        bench = sys.argv[4]
    if (len(sys.argv)) > 5:
        mac_num = int(sys.argv[5])
    args += (" -p %d" % mac_num)

    base_cmd = (BASE_CMD % (exe, bench, config_file)) + " --id %d " + args
    return


# print "starting with config file  %s" % config_file
def parse_bench_parameters(f):
    assert(false)
    global mac_set, mac_num
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "bench"

    mac_set = []
    for e in root.find("servers").find("mapping").findall("a"):
        mac_set.append(e.text.strip())
    return

def parse_hosts(f):
    global mac_set, pwd
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "hosts"

    mac_set = []
    black_list = {}

    # parse black list
    for e in root.find("black").findall("a"):
       black_list[e.text.strip()] = True
    # parse hosts
    for e in root.find("macs").findall("a"):
        server = e.text.strip()
        if not black_list.has_key(server):
            mac_set.append(server)
    pwd = root.find("pwd").text

    return

def start_servers(macset, config, bcmd,num):
    assert(len(macset) >= num)
    for i in xrange(1,num):
        cmd = (bcmd % (i)) + OUTPUT_CMD_LOG ## disable remote output
        subprocess.call(["ssh","-n","-f",macset[i],"rm *.log"],stdout=FNULL,stderr=subprocess.STDOUT) ## clean remaining log
        subprocess.call(["ssh", "-n","-f", macset[i], cmd],stdout=FNULL,stderr=subprocess.STDOUT)
    ## local process is executed right here
    ## cmd = "perf stat " + (bcmd % 0)
    cmd = bcmd % 0
    print cmd
    subprocess.call(cmd.split()) ## init local command for debug
    #subprocess.call(["ssh","-n","-f",macset[0],cmd])
    return

def prepare_files(files):
    print "Start preparing start file"
    global mac_num

    cached_file_stat = {}
    try:
        cache = open("run2.cache")
        cached_file_stat = pickle.load(cache)
        cache.close()
    except:
        pass
    if not cached_file_stat.has_key("mac_num"):
        cached_file_stat["mac_num"] = -1
    print cached_file_stat

    need_copy = False
    for f in files:
        ## check whether file has changed or not
        if cached_file_stat.has_key(f) \
           and cached_file_stat[f] == os.stat(f).st_mtime \
           and cached_file_stat["mac_num"] >= mac_num:
            continue
        need_copy = True
        cached_file_stat[f] = os.stat(f).st_mtime

    for f in files:
        copy_file(f)

    cache = open("run2.cache","w")
    cached_file_stat["mac_num"] = mac_num
    pickle.dump(cached_file_stat,cache)
    cache.close()
    return



#====================================#
## main function
def main():

    global base_cmd
    parse_input() ## parse input from command line
    parse_hosts("hosts.xml")

    for h in mac_set:
        p = ConnectProxy(h)
        p.connect(pwd)
        proxies.append(p)

    print "[START] Input parsing done."

    #kill_servers(exe)
    prepare_files([exe,config_file,"hosts.xml"])    ## copy related files to remote

    print "[START] cleaning remaining processes."

    time.sleep(1) ## ensure that all related processes are cleaned

    signal.signal(signal.SIGINT, signal_int_handler) ## register signal interrupt handler
    start_servers(mac_set, config_file, base_cmd,mac_num) ## start server processes
    for i in xrange(10):
        ## forever loop
        time.sleep(10)
    signal_int_handler(0,1) ## 0,1 are dummy args
    return

#====================================#
## the code
if __name__ == "__main__":
    main()
