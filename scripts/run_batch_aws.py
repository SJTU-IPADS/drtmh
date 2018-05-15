#!/usr/bin/env python

# This sciript is intend to run multiple tests in background.
# It also contains some utilies to check mac states, such as whether huge page is enabled
# at the desired server.

###############################################################################
#                                   usage:
#    ./run_test2.py trace_file       // the trace file format is below
#    ./run_test2.py config_file flag // check whether the mac defined in the config file satisfy the running requirments.
#    The host server must be the same as every config file's first node.(current limitation)
###############################################################################

## print from python3
from __future__ import print_function

import sys
import os
import commands
import subprocess
import time
import xml.etree.cElementTree as ET

from optparse import OptionParser

from subprocess import Popen, PIPE

## user defined packages
from run_util import print_with_tag
from run_util import bcolors
from run_util import PrintTee
from run_util import change_to_parent

ssh_base = ["ssh","-i", "../aws/tp.pem","-o","StrictHostKeyChecking=no","-n","-f"]

## a sample trace xml file
"""
<trace>
  <exes>
    <a> noccrad </a> // which exe to run
  </exes>
  <configs>
    <a> config2.xml</a> // which server to run
  </configs>
  <setting>
    <a>1 1</a>    // thread number [][]routine number
    <a>2 1</a>
    <a>4 1</a>
    <a>6 1</a>
    <a>8 1</a>
    <a>10 1</a>
    <a>12 1</a>
  </setting>
  <benchs>
    <a>tpce</a> // which benchmark to run
  </benchs>
  <params>
   <#name>#variable</#name>
  </params>
</trace>
"""
# constants ###################################################################

FNULL = open(os.devnull, 'w')
RETRY_COUNTS = 3
WARM_TIME = 60

## for TPC-E and Smallbank, end_epoch = 55, warm_epoch = 15
## for TPC-C, end_epoch = 15,warm_epoch = 5
END_EPOCH = 20
WARM_EPOCH = 5
#END_EPOCH = 55
#WARM_EPOCH = 15

# global variables  ###########################################################
results = {}
start = time.time()
prev_exe = "noccocc"

# defined functions##############################################################################

def add(tup1,tup2):
    # add 2 tuples
    res = []
    for i in xrange(0,len(tup1)):
        res.append(tup1[i] + tup2[i])
    return res

def kill_servers(mac_set,exe):
    #  print "ending ... kill servers..."
    kill_cmd = "pkill %s" % exe
    kill_cmd1 = "pkill %s --signal 2" % exe
    kill_cmd2 = "sudo pkill %s" % exe

    for i in xrange(len(mac_set)):
        subprocess.call( ssh_base + [mac_set[i], kill_cmd1])
        time.sleep(1)
        try:
            subprocess.call(ssh_base + [mac_set[i], kill_cmd2])
            bcmd = "ps aux | grep nocc"
            stdout, stderr = Popen(ssh_base + ["-o","ConnectTimeout=1",m, bcmd],
                                   stdout=PIPE).communicate()
            assert(len(stdout.split("\n")) == 3)

        except:
            pass

    time.sleep(2)

    return

def parse_config(config_file):
    mac_set = []
    tree = ET.ElementTree(file=config_file)
    root = tree.getroot()
    assert root.tag == "bench"

    machines = tree.find('servers')

    for e in machines.find("mapping").findall("a"):
        mac_set.append(e.text.strip())
    return mac_set

def copy_files(exe,config_file,mac_set):
    ## copy relate files to mac_set
    for host in mac_set:
        subprocess.call(["scp", "-i","../aws/tp.pem",config_file,"%s:%s" % (host,"~")],stdout=FNULL, stderr=subprocess.STDOUT)
        subprocess.call(["scp", "-i","../aws/tp.pem",exe,"%s:%s" % (host,"~")],stdout=FNULL, stderr=subprocess.STDOUT)
    return


def run_one_trace(exe,config_file,bench,threads,routines,remote):

    global prev_exe
    mac_set = parse_config(config_file)

    #print_with_tag("init","clean existing exes")
    kill_servers(mac_set,exe)
    prev_exe = exe

    #print_with_tag("init","copy files")
    copy_files(exe,config_file,mac_set)

    ## start the trace
    print_with_tag("run","run [%s] trace using [%s]:  c %d, t %d to %s" % \
                   (bench,exe,routines,threads,config_file))

    for i in xrange(len(mac_set)) :
        base_cmd = """ ./%s --bench %s  --id %d --config %s -t %d -c %d -r %d 1>log 2>&1 &""" \
                % (exe,bench,i,config_file,threads,routines,remote)
        subprocess.call(ssh_base + [mac_set[i],base_cmd],
                        stdout=FNULL, stderr=subprocess.STDOUT) ## hide the output
        #time.sleep(0.05)
    return mac_set

def init_checks(config,arg):
    mac_set = parse_config(config)
    try:
        arg = int(arg)
        if arg == 1:
        ## check whether huge pages are set
            return check_huge_page(mac_set)
        if arg == 2:
        ## check whether there are remaining processes of running
            check_process_status(mac_set)
            kill_servers(mac_set,"noccsi")
            return
        if arg == 3:
            return check_logs(mac_set)
    except:
        kill_servers(mac_set,arg)

def check_process_status(mac_set):
    failed_mac_set = {}
    for m in mac_set:
        print_with_tag("check","check running status %s" % m)
        bcmd = "ps aux | grep nocc"
        stdout, stderr = Popen(ssh_base + ["-o","ConnectTimeout=1",m, bcmd],
                               stdout=PIPE).communicate()
        if True:
            print(len(stdout.split("\n")))
        print(stdout)

    for k in failed_mac_set:
        print_with_tag("check","mac %s's process not running" % (k,failed_mac_set[k]))

    if len(failed_mac_set) == 0:
        print_with_tag("check","All process active.")

    return

def check_huge_page(mac_set):
    failed_mac_set = {}

    for m in mac_set:
        print_with_tag("check","check mac %s" % m)
        bcmd = "cat  /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
        stdout, stderr = Popen(ssh_base + ["-o","ConnectTimeout=1",m, bcmd],
                               stdout=PIPE).communicate()
        huge_page_num = int(stdout)
        if (huge_page_num < 8192):
            failed_mac_set[m] = huge_page_num
        print_with_tag("check","mac %s huge page %f" % (m,huge_page_num))

    for k in failed_mac_set:
        print_with_tag("check","mac %s huge page not enough %d" % (k,failed_mac_set[k]))

    if len(failed_mac_set) == 0:
        print_with_tag("check","Huge page check passes.")
    return

def check_logs(mac_set):
    failed_mac_set = {}

    for m in mac_set:
        print_with_tag("check","check mac %s" % m)
        bcmd = "cat  log"
        stdout, stderr = Popen(ssh_base + ["-o","ConnectTimeout=1",m, bcmd],
                               stdout=PIPE).communicate()
        print(stdout)
    return



def calculate_results(log_file_name):
    ## the desired return
    thr  = 0.0
    aborts = 0
    abort_ratio = 0
    latency = 0.0

    default_lats = [0,0,0,0]

    c = 0 ##counts
    count = 0
    last_line = ""
    ## open the file
    f = None
    try:
        f = open(log_file_name)
        for line in f:
            last_line = line
            ## exit condition
            if c >= END_EPOCH:
                if thr == 0:
                    ## avoids 0 results
                    print_with_tag("res","error 0")
                    return (0,0,default_lats,0)
                c += 1
                continue

            ## warmup
            if (c < WARM_EPOCH):
                c += 1
                continue
            try:
                s = line.split(" ")
                if (len(s) == 3):
                    thr += float(s[0])
                    abort_ratio += float(s[1])
                    aborts += float(s[2])
                    #latency += float(s[3])
                    count += 1
            except:
                c += 1
    except:
        return (0,0,default_lats,0)
    if (count == 0):
        return (0,0,default_lats,0)
    thr = thr / float(count)
    aborts = aborts / float(count)
    abort_ratio = abort_ratio / float(count)

    lats = default_lats
    try:
        lats = map(float,last_line.split(" "))
    except:
        pass

    f.close()
    return (thr / 1000.0,1.0 - abort_ratio,1.0 - abort_ratio,lats)

def parse_augment(tree):
    ## check whether augment is needed
    ## return a diction of params
    root_tag = "params"
    root = tree.getroot()
    params = root.find(root_tag)

    res = {}
    try:
        for e in params.iter():
            if e.tag == root_tag:
                continue ## root tag in the list, too
            res[e.tag] = e.text
    except:
        return res
    return res

def augment_config(params,config):
    # add specific parameters to the config file
    et = ET.ElementTree(file=config)
    for k in params:
        root = et.getroot()
        ## makes an overwrite
        try:
            root.remove(root.find(k))
        except:
            pass ## not exsists, just pass
        new_tag = ET.SubElement(et.getroot(), k)
        new_tag.text = params[k]
    et.write(config)
    return

def sanity_checks():
    global RETRY_COUNTS, END_EPOCH
    assert( RETRY_COUNTS > 0 and RETRY_COUNTS <= 5 )
    assert( END_EPOCH > 0 and END_EPOCH < 120 )

def hour_to_minutes(h):
    minute = 60
    hour = minute * 60
    return h * minute

def sleep_hours(h):
    minutes = hour_to_minutes(h)
    i = 0
    while (i < minutes):
        time.sleep(60)
        i += 1

# helper functions end##############################################################################

def main():

    global WARM_TIME

    trace_file = ""
    sleep_sec = 0

    ## parse sys.in
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="trace_file",
                  help="test trace file name", default="test_trail.xml")
    parser.add_option("-c","--command",dest="command",
                      help="command used for running", default=0)
    parser.add_option("-w","--warm time",dest="warm",
                      help="gap between each run", default=WARM_TIME)
    parser.add_option("-s","--sleep time",dest="sleep",
                      help="when the script will start", default=sleep_sec)
    parser.add_option("-r","--retry",dest="retry",
                      help="number of tests needed", default=3)

    (options, args) = parser.parse_args()

    trace_file = options.trace_file
    command = int(options.command)
    WARM_TIME = float(options.warm)
    sleep_sec = options.sleep
    RETRY_COUNTS = int(options.retry)

    print_with_tag("init","Start using trace_file %s, retry num %d" % (trace_file,RETRY_COUNTS))

    if (command != 0):
        init_checks(trace_file,command)
        exit()

    sleep_hours(sleep_sec) ## timer

    run_flag = True

    tree = ET.ElementTree(file = trace_file)
    root = tree.getroot()
    assert root.tag == "trace"

    params = parse_augment(tree)

    ## parse the trace content
    try:
        global RETRY_COUNTS
        retry_e = tree.find("retry")
        RETRY_COUNTS = int(retry_e.txt.strip())
    except:
        pass # use the default setting
    exes = []
    for e in tree.find("exes").findall("a"):
        exes.append(e.text.strip())
    configs = []
    for c in tree.find("configs").findall("a"):
        augment_config(params,c.text.strip())
        configs.append(c.text.strip())

    settings = []
    for c in tree.find("setting").findall("a"):
        line = c.text.strip()
        line = line.split()

        threads = int(line[0])
        routines = int(line[1])
        remote   = 1

        if len(line) > 2:
            remote = int(line[2])

        settings.append((threads,routines,remote))

    benchs = []
    for b in tree.find("benchs").findall("a"):
        benchs.append(b.text.strip())

    sanity_checks()

    log_dir = os.getenv("HOME") + "/results/"
    global results

    ## parse the result
    ## setting the result both to a log file
    f = open('s.log', 'w')
    backup = sys.stdout
    sys.stdout = PrintTee(sys.stdout,f)


    if run_flag:
        for b in benchs:
            for e in exes:
                for config in configs:
                    for s in settings:
                        mac_set = parse_config(config)
                        t,c,r = s
                        err_count = 0
                        retry = 0
                        log_file_name = "%s_%s_%lu_%lu_%d_%d.log" % (e,b,len(mac_set),t,c,r)

                        while (True):
                            elapsed = (time.time() - start)
                            start_txt = "start one trace, %d second passed" % elapsed
#                            print_with_tag("run %d" % retry,start_txt)
                            mac_set = run_one_trace(e,config,b,t,c,r)

                            time.sleep(WARM_TIME) ## for killing if possible execution
                            time.sleep(60) ## for data loading
                            time.sleep(20) ## for ending
                            kill_servers(mac_set,e)

                            ## check runnig status

                            res = (calculate_results(log_dir + log_file_name))
                            thr  = res[0]
                            rate = res[1]
                            lat  = res[3]
                            print("thr %s, " % thr,lat," @%d" % (retry))

                            if thr > 0:
                               ## merge running log to existing result file
                               mv_cmd = "cat %s >> %s" % ((os.getenv("HOME") + "/log"),(log_dir + log_file_name))
                               os.system(mv_cmd)
                            else:
                                err_count += 1
                                if err_count == RETRY_COUNTS + 2:
                                    print_with_tag("fatal","%s error!" % log_file_name)
                                    exit(0)
                                time.sleep(10) ## sleeping
                                continue
                            retry += 1

                            try: ## check whether results exists
                                temp = results[log_file_name]
                            except:
                                results[log_file_name] = [0.0,0.0,0.0,[0,0,0,0]]
                            #print(results[log_file_name])
                            (results[log_file_name])[0] += thr
                            (results[log_file_name])[1] += rate
                            try:
                                (results[log_file_name])[3] = add(lat,(results[log_file_name])[3])
#                                print_with_tag(log_file_name,"thr %f, results sum %f" % (thr,(results[log_file_name])[0]))
                            except:
                                if retry > RETRY_COUNTS + 2:
                                    print_with_tag("failed","xx")
                                    exit(0)
                                pass
                            if (retry == RETRY_COUNTS):
                                break




    for b in benchs:
        print("%s:" % (b))
        for e in exes:
            print("exe: %s" % e)
            for config in configs:
                for s in settings:
                    t,c,r = s
                    mac_set = parse_config(config)
                    ## caluculate the log file
                    log_file_name = "%s_%s_%lu_%lu_%d_%d.log" % (e,b,len(mac_set),t,c,r)
                    if run_flag:
                        ## print the batch
                        res  = results[log_file_name][0] / float(RETRY_COUNTS)
                        rate = results[log_file_name][1] / float(RETRY_COUNTS)
                        lat  = results[log_file_name][3]
                        for i in xrange(len(lat)):
                            lat[i] = lat[i] / float(RETRY_COUNTS)
                        print("%f %f %f %f %f %s %s" % (res,lat[0],lat[1],lat[2],lat[3],rate,config))
                    else:
                        # print the log file results
                        res = (calculate_results(log_dir + log_file_name))
                        thr = res[0]
                        lat = res[3]
                        print("%d %d %d -> %f, lat %f" % (len(mac_set),t,c,thr,lat))
                #print("") ## break line
            print("") ## break line

    f.close() ## close the log file
    sys.stdout = backup
    return

# comment #####################################################################

if __name__ == "__main__":
    main()
