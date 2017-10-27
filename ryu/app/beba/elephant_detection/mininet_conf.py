from mininet.topo import Topo
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.node import UserSwitch, RemoteController
from beba import BebaHost, BebaSwitchDbg
import os,time,pickle
import networkx as nx
import sys


C_list = list()
endpoint_list = list()
export_data = list()


def load_topo_info(filename):
    next_structure_flag = False
    with open(filename, "r") as fh:
        for line in fh:
            if not line.strip():
                next_structure_flag=True
            elif next_structure_flag:
                endpoint_list.append(tuple(line.split()))
            else:
                C_list.append(tuple(line.split()))

def is_switch(s):
    return s[0] == "s"

class MyTopo( Topo ):
    def build( self):
        nodes_set = set()
        G = nx.DiGraph()
        for el in C_list:
            nodes_set.add(el[0])
            nodes_set.add(el[1])
        G.add_nodes_from(list(nodes_set))
        for node in nodes_set:
            self.addSwitch(node) if is_switch(node) else self.addHost(node)
        for el in C_list:
            self.addLink(el[0], el[1], bw=int(el[2]), max_queue_size=10, use_htb=True)
            port_tuple = self.port(el[0], el[1])
            G.add_edge(el[0], el[1], bw=int(el[2]), port=port_tuple[0])
            G.add_edge(el[1], el[0], bw= int(el[2]), port= port_tuple[1])
        export_data.append(G)
        #print("nodi",G.nodes())
        #print("links", G.edges())

def get_hosts_info(net,topo):
    d = dict()
    hosts = topo.hosts()
    for host in hosts:
        temp = dict()
        temp["ip"] = net.get(host).IP()
        temp["mac"] = net.get(host).MAC()
        d[host] = temp
    return d


def build_iperf_cmd(proto, bw, i, srv_add):
    base_port = 3000
    conn_time = 40
    s= "iperf3 " if proto == "TCP" else "iperf "

    if srv_add != "":
        s+= "-c" + " " + srv_add + " " + "-t" + " " + str(conn_time) + " "
        if proto == "UDP":
            s += "-u" + " "
        if bw != 0:
            s+= "-b" + " " + str(bw) + " "
    else:
        s+= "-s" + " "

    s+= "-p" + " " + str(base_port + i) + " "
    s+= "&"
    print(s)
    return s


if __name__ == '__main__':
    assert len(sys.argv) == 2
    load_topo_info(sys.argv[1])

    is_nat_active = False
    debug_mode = False
    if os.geteuid() != 0:
        exit("You need to have root privileges to run this script")
    os.system("sudo mn -c 2> /dev/null")
    os.system("kill -9 $(pidof -x ryu-manager) 2> /dev/null")

    print 'Starting Ryu controller'
    topo = MyTopo()
    net = Mininet(topo= topo,
                  host=BebaHost,
                  ipBase='10.0.0.0/8',
                  link=TCLink,
                  switch=UserSwitch if not debug_mode else BebaSwitchDbg,
                  controller=RemoteController,
                  cleanup=True,
                  autoSetMacs=True,
                  autoStaticArp=True,
                  listenPort=6634)
    if is_nat_active:
        net.addNAT().configDefault()
        for off in ["rx", "tx", "sg"]:
            cmd = "/sbin/ethtool --offload nat0-eth0 %s off" % off
            net.hosts[-1].cmd(cmd)
    hosts_info = get_hosts_info(net, topo)
    n_hosts = len(hosts_info)
    #print(hosts_info)
    export_data.append(hosts_info)
    with open('data.pkl', 'wb') as fh:
        pickle.dump(export_data, fh)

    #os.system('ryu-manager mainapp.py 2> /dev/null &')
    os.system('ryu-manager mainapp.py &')
    #os.system('xterm -e "ryu-manager mainapp.py; bash" &')
    net.start()
    time.sleep(5)
    #CLI( net )
    #raw_input('Press ENTER to start iperf...')
    for i,connection in enumerate(endpoint_list):
        server_host = net.get(connection[1])
        server_ip = server_host.IP()
        res = server_host.cmd (build_iperf_cmd(connection[2], connection[3], i, "" ))
        time.sleep(2)
        net.get(connection[0]).cmd (build_iperf_cmd(connection[2], connection[3], i, server_ip ))
        time.sleep(2)
    time.sleep(40)
    net.stop()
    os.system("sudo mn -c 2> /dev/null")
    os.system("kill -9 $(pidof -x ryu-manager) 2> /dev/null")