import xml.etree.cElementTree as ET ## parsing the xml file
import subprocess
import os
import pickle
import itertools
from subprocess import Popen, PIPE

def gen_key(params):
    key = "%s_%s_%lu_%lu_%d_%d.log" \
          % (params["exe"],params["bench"],params["m"],params["t"],params["c"],params["r"])
    return key

def parse_port(config_file):

    tree = ET.ElementTree(file = config_file)
    root = tree.getroot()

    port = 8888
    for e in root.findall("port"):
        port = int(e.text)

    ## update
    try:
        root.remove(root.find("port"))
    except:
        pass ## not exsists, just pass
    new_tag = ET.SubElement(tree.getroot(), "port")
    new_tag.text = str(port + 1)
    tree.write(config_file)

    print("Use port " + str(port))
    return port

class RoccRunner(object):

    def __init__(self,trace_file="test_trail.xml",warm_epoch=0):
        self.traces = []  ## run command
        self.trace_file = trace_file
        self.results = [] ## run results
        self.results_idx = 0
        self.warm_epoch = warm_epoch ## record results from the @warm_epoch@ entry in the log file

    def parse_trace(self):
        ## clear traces
        self.traces = []

        tree = ET.ElementTree(file = self.trace_file)
        root = tree.getroot()
        assert root.tag == "trace"

        ## parse executables
        exes = []
        for e in tree.find("exes").findall("a"):
            exes.append(e.text.strip())

        ## parse configure files
        configs = []
        for c in tree.find("configs").findall("a"):
            configs.append(c.text.strip())

        macs = []
        for m in tree.find("macs").findall("a"):
            macs.append(int(m.text))
        macs.sort()

        thread_nums = []
        for t in tree.find("threads").findall("a"):
            thread_nums.append(int(t.text))
        thread_nums.sort()

        coroutine_nums = []
        for c in tree.find("coroutines").findall("a"):
            coroutine_nums.append(int(c.text))
        coroutine_nums.sort()

        r_configs = []
        for r in tree.find("r").findall("a"):
            r_configs.append(int(r.text))

        benchs = []
        for b in tree.find("benchs").findall("a"):
            benchs.append(b.text.strip())

        combinations = [exes,benchs,configs,macs,thread_nums,coroutine_nums,r_configs]

        for l in itertools.product(*combinations):
            e,b,config,m,t,c,r = l
            self.traces.append({"exe":e, "bench":b, "config":config,\
                                "m":  m, "t":t,"c":c,"r":r} )
        print("Total %d traces parsed." % len(self.traces))
        return

    def check_liveness(self,params,host):
        bcmd = "pgrep %s" % "nocc"
        stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=1",host, bcmd],
                               stdout=PIPE).communicate()
        if len(stdout) > 0: ## process found
            return True
        return False

    def kill_instances(self,params,hosts):
        kill_cmd = "pkill %s" % params["exe"]
        for m in hosts:
            subprocess.call(["ssh", "-n","-f", m, kill_cmd])
        pass

    def get_next_cmd(self,params):

        parse_port(params["config"])

        base_cmd = './%s --bench %s --config %s -t %d -c %d -r %d -p %d'
        base_cmd = base_cmd % (params["exe"],params["bench"],params["config"],\
                               params["t"],params["c"],params["r"],params["m"])
        base_cmd += " --id %d"
        print(base_cmd)
        return base_cmd

    ## store the last-run results to disk
    def dump_results(self,fname):
        f = open(fname,"w")
        pickle.dump(self.results,f)
        f.close()

    def append_res(self,params,seq):

        thr = 0.0
        m_l = 0.0
        fl = 0.0
        nl = 0.0
        avg_l = 0.0

        log_file_name = gen_key(params)

        log_dir = os.getenv("HOME") + "/results/"
        count = 0
        last_line = ""

        try:
            f = open(log_dir + log_file_name)
        except:
            print("File open error " + log_file_name)
            return False

        try:
            c = 0
            for line in f:
                c += 1
                last_line = line
                if c < self.warm_epoch:
                    continue
                try:
                    s = line.split(" ")
                    if (len(s) == 3):
                        thr += float(s[0])
                        count += 1
                except:
                    pass
            f.close()
        except:
            pass
        if count != 0:
            thr = thr / float(count)
        s = last_line.split(" ")
        try:
            m_l = float(s[0])
            fl = float(s[1]) ## five
            nl = float(s[2]) ## nine
            avg_l = float(s[3])
        except:
            pass
        res = {"res":{"thr":thr,"medium-lat":m_l,"50-lat":fl,"90-lat":nl,"avg-lat":avg_l},"params":params}
        print(thr,m_l,fl,nl,avg_l,count)
        self.results.append(res)
        mv_cmd = "mv " + os.getenv("HOME") + "/log" + " " + (log_file_name + str(seq))
        print(mv_cmd)
        os.system(mv_cmd)
        return thr != 0

    ## the Runner can get trace in
    def __iter__ (self):
        return self

    def next(self):
        if self.results_idx >= len(self.traces):
            ## no further traces
            raise StopIteration
        else:
            print("Process %d / %d" % (self.results_idx,len(self.traces)))
            params = self.traces[self.results_idx]
            self.results_idx += 1
            return params

    def reset(self):
        self.results_idx = 0
