#!/usr/bin/env python

import sys
import os
import commands
import subprocess
import json
import xml.etree.cElementTree as ET

from optparse import OptionParser

from subprocess import Popen, PIPE

start_instances      = ["aws","ec2", "run-instances","--image-id","ami-b7ead9d2","--instance-type","r4.2xlarge",\
                        "--key-name","tp","--monitoring",'{"Enabled":false}',\
                        "--security-group-ids","sg-2131064a",\
                        "--instance-initiated-shutdown-behavior","stop",
                        "--subnet-id","subnet-fec85696",\
                        "--count"]
query_instances      = ["aws","ec2","describe-instances","--query","Reservations[*].Instances[*].InstanceId"]
terminate_instances  = ["aws","ec2", "terminate-instances", "--instance-ids"]
query_ip             = ["aws","ec2", "describe-instances", "--query",'Reservations[*].Instances[*].NetworkInterfaces[*]']

def transfer_ids(ids):
    res = []
    for i in ids:
        if type(i) == list:
            res += transfer_ids(i)
        else:
            res.append(i)
    return res

def start(num):
    check_num = int(num)
    assert check_num >= 1 and check_num < 60
    start_instances.append(str(num))
    print start_instances
    subprocess.call(start_instances)

def get_ids():
    stdout, stderr = Popen(query_instances,
                           stdout=PIPE).communicate()

    ids = json.loads(stdout)
    return transfer_ids(ids)


def kill_instances(ids):
    subprocess.call(terminate_instances + ids)


def get_ips():
    stdout,stderr = Popen(query_ip,stdout = PIPE).communicate()
    ips = json.loads(stdout)
    res = []
    for i in ips:
        for j in i:
            try:
                private_ip =  j[0][u'PrivateIpAddress']
                public_ip  =  j[0][u'Association'][u'PublicIp']
                res.append((public_ip,private_ip))
            except:
                pass

    return res

def generate_config(ips):

    et = ET.ElementTree(file="../config_template.xml")
    root = et.getroot()
    servers =  root.find("servers")

    total_mac = len(ips)
    c = 0

    for i in ips:
        c += 1
        public,private = i

        new_tag = ET.SubElement(servers.find("mapping"), "a")
        new_tag.text = private
        et.write("config%s.xml" % c)
    print ips[0][0]

# main hook
def main():
    wether_kill = False
    whether_ip  = False
    whether_id  = False
    whether_ge  = False
    whether_start = 0

    parser = OptionParser()
    parser.add_option("-s", "--start", dest="whether_start",
                      help="kill aws instances",default=0)
    parser.add_option("-k", "--kill", dest="whether_kill",action="store_true",
                      help="kill aws instances",default=False)
    parser.add_option("-q", "--ip",dest="whether_ip",action="store_true",
                      help="query aws instances ips",default = False)
    parser.add_option("-i","--id", dest="whether_id",action="store_true",
                      help="query aws instances ids",default = False)
    parser.add_option("-g","--gen", dest="whether_ge",action="store_true",
                      help="whether to generate config files",default = False)


    (options, args) = parser.parse_args()
    whether_id = options.whether_id
    whether_ip = options.whether_ip
    whether_kill = options.whether_kill
    whether_ge   = options.whether_ge
    whether_start = options.whether_start

    if whether_id:
        ids = get_ids()
        print ids
    if whether_ip:
        print get_ips()
    if whether_kill:
        ids = get_ids()
        kill_instances(ids)
    if whether_ge:
        generate_config(get_ips())
    if whether_start > 0:
        start(whether_start)

main()
