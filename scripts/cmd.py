from subprocess import Popen, PIPE
import subprocess # execute commands
import os  # change dir

FNULL = open(os.devnull, 'w')

def execute_at(mac,cmd):
    subprocess.call(["ssh", "-n","-f", mac, cmd])

def slient_execute_at(mac,cmd):
    subprocess.call(["ssh", "-n","-f", mac, cmd],stdout=FNULL,stderr=subprocess.STDOUT)

def get_w_execute(mac,cmd,timeout = 2):
    stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=" + int(timeout),mac,cmd],
                           stdout=PIPE).communicate()
    return stdout,stderr
