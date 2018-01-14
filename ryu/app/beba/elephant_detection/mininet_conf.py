from mininet.topo import Topo
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.node import UserSwitch, RemoteController
from beba import BebaHost, BebaSwitchDbg
import os,time,pickle
import networkx as nx
import sys

'''
We are assunimng Ryu is running on a VM reachable via 6634 (OpenFlow) and 4567 (SSH).
data.pkl is automatically copied via scp (root login via SSH must be enabled)
'''

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


def build_iperf_cmd(proto, bw, i, srv_add, conn_time=600, use_xterm=False):
    if False:
        base_port = 3000
        s= "iperf3 " if proto == "TCP" else "iperf "

        if srv_add != "":
            # client instance
            s = 'sleep 1; ' + s
            s+= "-c" + " " + srv_add + " " + "-t" + " " + str(conn_time) + " "
            if proto == "UDP":
                s += "-u" + " "
            if bw != "0":
                s+= "-b" + " " + str(bw) + " "
        else:
            # server instance
            s+= "-s" + " "

        s+= "-p" + " " + str(base_port + i) + " "
        s+= "&"
    else:
        '''
        cd ~
        mkdir nuttcp
        cd nuttcp/
        wget http://nuttcp.net/nuttcp/beta/nuttcp-7.3.3.c
        cc nuttcp-7.3.3.c -o nuttcp-7
        sudo cp nuttcp-7 /usr/bin/nuttcp
        '''
        base_port = 7000
        if srv_add != "":
            s = '(sleep 1; nuttcp -P' + str(base_port + i) + ' -v -T ' + str(conn_time) + ' '
            if proto != "TCP":
                s += '-u '
            if bw != "0":
                s += "-Ri" + str(bw.lower()) + " "
            s += ' ' + srv_add + ') &'
        else:
            s = 'nuttcp -v -1 -P' + str(base_port + i)
            if proto != "TCP":
                s += ' -u '
            s += '&'

    if use_xterm:
        XTERM_GEOMETRY = '-geometry 80x20+100+100'
        s = 'xterm %s -e "%s; bash"&' % (XTERM_GEOMETRY, s.replace('&', ''))

    print(s)
    return s


if __name__ == '__main__':
    assert len(sys.argv) == 2
    load_topo_info(sys.argv[1])

    # Set to True to create M CBR flows and N elastic flows with an automated topology
    OVERRIDE = True

    # Set to True if ryu is running inside a VM
    REMOTE_CTRL = True

    if OVERRIDE:
        M_RANGE = [1]
        N_RANGE = [6]
        IPERF_DURATION = 60  # s
        ACCESS_LINK_CAPACITY = 20 # Mbps
        CORE_LINK_CAPACITY = 100  # Mbps
        CBR = '20M'
    else:
        M_RANGE = [0]
        N_RANGE = [0]
        IPERF_DURATION = 600

    for M in M_RANGE:
        for N in N_RANGE:
            if OVERRIDE:
                # override topology
                C_list = [('s100', 's200', str(CORE_LINK_CAPACITY))]
                endpoint_list = []
                for idx, i in enumerate(range(N)):
                    C_list.append(('h%d' % idx, 's100', str(ACCESS_LINK_CAPACITY)))
                    C_list.append(('h%d' % (201+idx), 's200', str(ACCESS_LINK_CAPACITY)))
                    endpoint_list.append(('h%d' % (idx), 'h%d' % (201+idx), 'TCP', '0', '0'))
                for idx, i in enumerate(range(M)):
                    C_list.append(('h%d' % (1000 + idx), 's100', str(ACCESS_LINK_CAPACITY)))
                    C_list.append(('h%d' % (1000 + 201 + idx), 's200', str(ACCESS_LINK_CAPACITY)))
                    endpoint_list.append(('h%d' % (1000 + idx), 'h%d' % (1000 + 201 + idx), 'UDP', CBR, '0'))
                # print C_list
                # print endpoint_list

            FILENAME = '%d.%d.txt' % (M, N)

            is_nat_active = False
            debug_mode = False
            if os.geteuid() != 0:
                exit("You need to have root privileges to run this script")
            os.system("sudo kill -9 `pidof nuttcp` 2> /dev/null")
            os.system("sudo kill -9 `pidof xterm` 2> /dev/null")
            os.system("sudo mn -c 2> /dev/null")
            if REMOTE_CTRL:
                os.system('sshpass -p mininet ssh -p 4567 root@0 killall ryu-manager')
            else:
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
            if REMOTE_CTRL:
                os.system('sshpass -p mininet scp -P 4567 data.pkl root@0:/home/mininet/beba-ctrl/ryu/app/beba/elephant_detection')

            #os.system('ryu-manager mainapp.py 2> /dev/null &')
            #os.system('ryu-manager mainapp.py &')
            #print 'Start Ryu'
            if REMOTE_CTRL:
                os.system('sshpass -p mininet ssh -X -p 4567 root@0 cd /home/mininet/beba-ctrl/ryu/app/beba/elephant_detection/\;xterm -e \"export\ FILENAME=%s\;ryu-manager\ mainapp.py\;bash\" &' % FILENAME)
            else:
                os.system('xterm -e "export FILENAME=%s; ryu-manager mainapp.py; bash" &' % FILENAME)
            net.start()
            time.sleep(5)
            #CLI( net )
            #raw_input('Press ENTER to start iperf...')
            endpoint_list = sorted(endpoint_list,key= lambda x: x[4], reverse=True)
            max_start_time = int(endpoint_list[0][4])
            i = 0
            t = 0
            while endpoint_list:
                if t >= int(endpoint_list[-1][4]):
                    connection = endpoint_list.pop()
                    server_host = net.get(connection[1])
                    server_ip = server_host.IP()
                    res = server_host.cmd (build_iperf_cmd(connection[2], connection[3], i, "", conn_time=IPERF_DURATION))
                    #time.sleep(2)
                    net.get(connection[0]).cmd (build_iperf_cmd(connection[2], connection[3], i, server_ip, conn_time=IPERF_DURATION))
                    #time.sleep(2)
                    i+=1
                else:
                    time.sleep(1)
                    t+=1

            time.sleep(10 + max_start_time + IPERF_DURATION)
            #CLI(net)
            os.system("sudo kill -9 `pidof nuttcp` 2> /dev/null")
            os.system("sudo kill -9 `pidof xterm` 2> /dev/null")
            net.stop()
            os.system("sudo mn -c 2> /dev/null")
            if REMOTE_CTRL:
                os.system('sshpass -p mininet ssh -p 4567 root@0 killall ryu-manager')
            else:
                os.system("kill -9 $(pidof -x ryu-manager) 2> /dev/null")
            time.sleep(5)
