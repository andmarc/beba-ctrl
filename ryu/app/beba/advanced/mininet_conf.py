from mininet.topo import Topo
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.node import UserSwitch, RemoteController
from beba import BebaHost, BebaSwitchDbg
import os,time,pickle
import networkx as nx

C_list = [('h1','s1',10),
              ('h2', 's1', 10),
              ('h3', 's3', 10),
              ('s1', 's2', 10),
              ('s1', 's3', 5),
              ('s2', 's3', 10)]

export_data = []


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
            self.addLink(el[0], el[1], bw=el[2], max_queue_size=10, use_htb=True)
            port_tuple = self.port(el[0], el[1])
            G.add_edge(el[0], el[1], bw=el[2], port=port_tuple[0])
            G.add_edge(el[1], el[0], bw= el[2], port= port_tuple[1])
        export_data.append(G)
        #print("nodi",G.nodes())
        #print("links", G.edges())

def get_hosts_info(net,topo):
    d = dict()
    hosts = topo.hosts()
    for host in hosts:
        d[host] = (net.get(host).MAC(), net.get(host).IP())
    return d

if __name__ == '__main__':

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

    #os.system('ryu-manager sample_and_hold_sync_monitoring.py 2> /dev/null &')
    os.system('xterm -e "ryu-manager sample_and_hold_sync_monitoring.py; bash" &')
    net.start()
    time.sleep(5)
    #CLI( net )
    raw_input('Press ENTER to start iperf...')
    server_host = net.get("h3")
    server_ip = server_host.IP()
    for i in range(n_hosts):
        if net.hosts[i] != server_host:
            server_host.cmd ("iperf -s -p %d &" % (1000+i))
            net.hosts[i].cmd ("iperf -c %s -p %d -t 50 &" % (server_ip, 1000+i))
    time.sleep(60)
    net.stop()
    os.system("sudo mn -c 2> /dev/null")
    os.system("kill -9 $(pidof -x ryu-manager) 2> /dev/null")