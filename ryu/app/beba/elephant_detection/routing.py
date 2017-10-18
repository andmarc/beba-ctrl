import networkx as nx
from utils import add_flow, dpid_from_name, get_from_mininet


class Routing():
    def __init__(self, devices, table_id, next_table_id, time_interval, congestion_thresh=95):

        self.topo, self.addresses = get_from_mininet()
        self.devices = devices
        self.table_id = table_id
        self.next_table_id = next_table_id
        self.time_interval = time_interval
        self.congestion_thresh = congestion_thresh

        self.switch_num = len([node for node in self.topo.nodes() if 's' in node])
        self.link_ep_map = dict()
        self.ep_path_map = dict()

    def update_link_ep_map(self, path, mode, is_init= False):
        for previous_hop, hop in list(zip(path, path[1:])):
            if mode == "ADD":
                self.link_ep_map.setdefault((previous_hop,hop),[])
                self.link_ep_map[(previous_hop,hop)].append((path[0],path[-1],is_init))
            elif mode == "REM":
                self.link_ep_map[(previous_hop, hop)].remove((path[0], path[-1],is_init))

    def update_capacity(self, path, topo, mode, elephant_size):
        path = path[1:-1]
        for previous_hop, hop in list(zip(path, path[1:])):
            if mode == "ADD":
                topo[previous_hop][hop]["C_res"]+= elephant_size
            elif mode == "REM":
                topo[previous_hop][hop]["C_res"]-= elephant_size


    def apply_routing(self, path, topo, priority):
        src_ip = self.addresses[path[0]]["ip"]
        dst_ip = self.addresses[path[-1]]["ip"]
        for previous_hop, hop, next_hop in list(zip(path, path[1:], path[2:])):
            dp = self.devices[dpid_from_name(hop)]
            parser = dp.ofproto_parser
            add_flow(dp, self.table_id, priority, parser.OFPMatch(eth_type=0x800,
                                                           ipv4_src=src_ip,
                                                           ipv4_dst=dst_ip,
                                                           in_port=topo[hop][previous_hop]['port']),
                          [parser.OFPActionOutput(topo[hop][next_hop]['port'])],
                          [parser.OFPInstructionGotoTable(self.next_table_id)])
        #self.link_ep_map.setdefault((hop, next_hop), [])
        #self.link_ep_map[(hop, next_hop)].append((path[0], path[-1]))

    def initialize_routing(self):
        if len(self.devices) == self.switch_num:
            hosts = [node for node in self.topo.nodes() if 'h' in node]
            for h1 in hosts:
                for h2 in hosts:
                    if h2 != h1:
                        try:
                            path = nx.shortest_path(self.topo, h1, h2)
                            print 'path %s->%s' % (h1, h2), path
                            self.apply_routing(path, self.topo, 0)
                            self.update_link_ep_map(path, "ADD", True)
                        except nx.NetworkXNoPath:
                            print "No path between %s and %s" % (h1, h2)

    def get_elephant_by_link(self,flow_stats_history, link):
        a,b = link
        dpid = dpid_from_name(a)
        #print(flow_stats_history[-1])
        if dpid not in flow_stats_history[-1]:
            return None
        flows = dict()
        for el in flow_stats_history[-1][dpid].values():
            flows.update(el)

        temp_map = [(el[0], el[1])
                    for el in self.link_ep_map[link]
                    if not ((el[0], el[1]) in self.ep_path_map and (el[0], el[1], False) not in self.link_ep_map[link])]

        link_ip_map = [(self.addresses[el[0]]["ip"],
                        self.addresses[el[1]]["ip"])
                       for el in temp_map]
        max_value = 0
        for flow in flows:
            if (flow[0],flow[1]) in link_ip_map and flows[flow][3] > max_value:
                max_value = flows[flow][3]
                elephant = temp_map[link_ip_map.index((flow[0],flow[1]))]
        return (elephant,max_value) if max_value else None


    def react(self, port_stats_history, flow_stats_history):
        topo1 = self.topo.copy()
        new_forwarding_list = set()
        for link in sorted(self.topo.edges()):
            if 'h' not in link[0] and 'h' not in link[1]:
                print
                print link

                port_data = port_stats_history[-1][dpid_from_name(link[0])][self.topo[link[0]][link[1]]['port']]
                rx_occ = 100 * port_data[0] * 8 / (self.topo[link[0]][link[1]]['bw'] * self.time_interval * 1e6)
                tx_occ = 100 * port_data[1] * 8 / (self.topo[link[0]][link[1]]['bw'] * self.time_interval * 1e6)
                print self.topo[link[0]][link[1]]['port'], "RX = %.2f%%, TX = %.2f%%" % (rx_occ, tx_occ)
                occ = tx_occ
                #for occ in (rx_occ, tx_occ):
                if occ > self.congestion_thresh:
                    link_ep = (link[0], link[1]) if occ == tx_occ else (link[1], link[0])
                    res = self.get_elephant_by_link(flow_stats_history, link_ep)
                    if res is not None:
                        new_forwarding_list.add(res)
                topo1[link[0]][link[1]]["C_res"] = (100 - tx_occ)/100 * (
                self.topo[link[0]][link[1]]['bw'] * self.time_interval * 1e6) / 8


        if new_forwarding_list:
            new_forwarding_list = sorted(new_forwarding_list, key=lambda x: x[1], reverse=True)
            #print "lista forwarding", new_forwarding_list
            #print topo1.edges(data=True)
            for el in new_forwarding_list:
                elephant_ep, elephant_size = el
                #print elephant_ep,elephant_size
                topo2 = topo1.copy()
                for link in sorted(topo2.edges()):
                    if 'h' not in link[0] and 'h' not in link[1]:
                        if topo1[link[0]][link[1]]["C_res"] <= elephant_size:
                            topo2.remove_edge(*link)
                try:
                    path = nx.shortest_path(topo2, *elephant_ep)
                    if elephant_ep not in self.ep_path_map:
                        priority_old = 0
                    else:
                        path_old, priority_old = self.ep_path_map[elephant_ep]
                        self.update_link_ep_map(path_old, "REM")
                        self.update_capacity(path_old, topo1, "REM", elephant_size)

                    self.update_link_ep_map(path, "ADD")
                    self.update_capacity(path, topo1, "ADD", elephant_size)
                    self.apply_routing(path, topo1, priority_old+1)
                    print 'path %s->%s' % (elephant_ep[0], elephant_ep[1]), path
                    print "Installed with priority", priority_old+1
                    self.ep_path_map[elephant_ep] = (path, priority_old+1)
                except nx.NetworkXNoPath:
                    print "No path between %s and %s" % (elephant_ep[0], elephant_ep[1])