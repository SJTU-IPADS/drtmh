###
## This file provides wrapper to help execute command at remote servers
###

from subprocess import Popen, PIPE
import subprocess # execute commands
import os  # change dir

import paramiko
import getpass

FNULL = open(os.devnull, 'w')

class ConnectProxy:
    def __init__(self,mac,user=""):
        if user == "":
            user = getpass.getuser()
        self.ssh = paramiko.SSHClient()

        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.user = user
        self.mac  = mac
        self.sftp = None

    def connect(self,pwd):
        self.ssh.connect(hostname = self.mac,username = self.user, password = pwd)

    def execute(self,cmd,pty=False):
        return self.ssh.exec_command(cmd,get_pty=pty)

    def copy_file(self,f,dst_dir = ""):
        if self.sftp == None:
            self.sftp = paramiko.SFTPClient.from_transport(self.ssh.get_transport())
        self.sftp.put(f, dst_dir + f)

    def get_file(self,remote_path,f):
        if self.sftp == None:
            self.sftp = paramiko.SFTPClient.from_transport(self.ssh.get_transport())
        self.sftp.get(remote_path + "/" + f,f)

    def close(self):
        if self.sftp != None:
            self.sftp.close()
        self.ssh.close()

def execute_at(mac,cmd):
    subprocess.call(["ssh", "-n","-f", mac, cmd])

def slient_execute_at(mac,cmd):
    subprocess.call(["ssh", "-n","-f", mac, cmd],stdout=FNULL,stderr=subprocess.STDOUT)

def get_w_execute(mac,cmd,timeout = 2):
    stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=" + int(timeout),mac,cmd],
                           stdout=PIPE).communicate()
    return stdout,stderr

def get_w_execute_aws(mac,cmd,timeout = 2,pem = "../aws/tp.pem"):
    stdout, stderr = Popen(['ssh',"-i",pem,"-o","ConnectTimeout=" + str(timeout),mac,cmd],
                           stdout=PIPE).communicate()
    return stdout,stderr

def execute_at_aws(mac,cmd,pem = "../aws/tp.pem"):
    ssh_base = ["ssh","-i",pem,"-o","StrictHostKeyChecking=no","-n","-f"]
    subprocess.call(ssh_base + [mac,cmd])

def copy_to(f,mac,dst="~"):
    subprocess.call(["scp",f,"%s:%s" % (mac,dst)],stdout=FNULL, stderr=subprocess.STDOUT)

def copy_to_aws(f,mac,dst = "~",pem = "../aws/tp.pem"):
    subprocess.call(["scp","-i",pem,f,"%s:%s" % (mac,dst)],stdout=FNULL, stderr=subprocess.STDOUT)
