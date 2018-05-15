## helper functions for various script

import os # for chdir

## print with color helper class
class bcolors:
    HEADER = '\033[5;%s;2m'
    OKBLUE = '34'
    OKRED  = '31'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @staticmethod
    def wrap_text_with_color(txt,c):
        return ("%s%s%s" % ((bcolors.HEADER % c),txt,bcolors.ENDC))

def print_with_tag(tag,s):
    tag = bcolors.wrap_text_with_color(tag,bcolors.OKRED)
    print "[%s] %s" % (tag,s)


class PrintTee(object):
    def __init__(self,*files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)

## end class bcolors

def change_to_parent():
    os.chdir("..")
    return
