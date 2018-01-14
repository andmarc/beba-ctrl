import networkx as nx
import operator
from utils import add_flow, dpid_from_name, get_from_mininet, get_switch_time, bps_to_human_string, red_msg, green_msg
import datetime
from pprint import pprint

class Routing():
    def __init__(self, devices, table_id, next_table_id, stats_req_time_interval, switch_window_time_interval,
                 congestion_thresh=95, filename=None):

        self.topo, self.addresses = get_from_mininet()
        self.devices = devices
        self.table_id = table_id
        self.next_table_id = next_table_id
        self.stats_req_time_interval = stats_req_time_interval
        self.switch_window_time_interval = switch_window_time_interval
        self.congestion_thresh = congestion_thresh
        self.time_interval_np = round(stats_req_time_interval * 6.5)

        self.switch_num = len([node for node in self.topo.nodes() if 's' in node])
        self.link_ep_map = dict()
        self.ep_path_map = dict()

        self.filename = filename
        if filename is not None:
            self.filename = datetime.datetime.now().strftime('%Y-%m-%d.%H:%M:%S.') + self.filename

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

    def remove_expired_path(self):
        current_time = get_switch_time()
        keys = self.ep_path_map.keys()
        for elephant_ep in keys:
            ts = self.ep_path_map[elephant_ep][2]
            if current_time > ts:
                path_old= self.ep_path_map[elephant_ep][0]
                self.update_link_ep_map(path_old, "REM")
                del self.ep_path_map[elephant_ep]

    def apply_routing(self, path, topo, priority, hard_timeout=0, idle_timeout=0):
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
                          [parser.OFPInstructionGotoTable(self.next_table_id)],
                     hard_timeout=hard_timeout, idle_timeout=idle_timeout)
        #self.link_ep_map.setdefault((hop, next_hop), [])
        #self.link_ep_map[(hop, next_hop)].append((path[0], path[-1]))

    def initialize_routing(self):
        if len(self.devices) == self.switch_num:
            print("Installing default routes")
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
                            red_msg("No path between %s and %s" % (h1, h2))

    def get_elephant_by_link(self,flow_stats_history, link):
        a,b = link
        dpid = dpid_from_name(a)
        #print(flow_stats_history[-1])
        if dpid not in flow_stats_history[-1]:
            return None
        #flows = dict()
        #for el in flow_stats_history[-1][dpid].values():
        #    flows.update(el)
        flows = flow_stats_history[-1][dpid]

        temp_map = [(el[0], el[1])
                    for el in self.link_ep_map[link]
                    if not ((el[0], el[1]) in self.ep_path_map and (el[0], el[1], False) not in self.link_ep_map[link])]

        link_ip_map = [(self.addresses[el[0]]["ip"],
                        self.addresses[el[1]]["ip"])
                       for el in temp_map]
        max_value = 0

        """for flow in flows:
            if (flow[0],flow[1]) in link_ip_map and flows[flow][3] > max_value:
                max_value = flows[flow][3]
                elephant = temp_map[link_ip_map.index((flow[0],flow[1]))]"""

        n_stats = len(flow_stats_history)
        all_elephants = {}
        for flow in flows:
            if (flow[0], flow[1]) in link_ip_map:
                ts_end_new = flows[flow][0]
                count_new = flows[flow][1]
                ts_end_old = flows[flow][2]
                count_ew =  flows[flow][3]
                offset_time_windows = int((ts_end_old - self.start_ts)/(self.stats_req_time_interval*1000))
                try:
                    count_old = flow_stats_history[offset_time_windows][dpid][flow][1]
                    real_rate = (count_new + count_ew - count_old)/\
                                ((n_stats-offset_time_windows)* self.stats_req_time_interval)
                except:
                    real_rate = (count_new + count_ew)/\
                                (self.start_ts/1000+n_stats*self.stats_req_time_interval + self.switch_window_time_interval - ts_end_old/1000)
                #value = real_rate * self.stats_req_time_interval
                #print(flow, value)
                all_elephants[temp_map[link_ip_map.index((flow[0], flow[1]))]] = real_rate
                if real_rate > max_value:
                    max_value = real_rate
                    elephant = temp_map[link_ip_map.index((flow[0], flow[1]))]

        return {elephant:max_value*8} if max_value else None, all_elephants if len(all_elephants)>0 else None


    def react(self, port_stats_history, flow_stats_history, start_ts):
        print
        print "Link occupation report"
        print "Link threshold is set to %s%%" % self.congestion_thresh

        self.start_ts = start_ts
        self.remove_expired_path()
        topo1 = self.topo.copy()
        new_forwarding_list = dict()
        all_elephants_of_saturated_links = {}
        for link in sorted(self.topo.edges()):
            if 'h' not in link[0] and 'h' not in link[1]:
                print
                print link

                port_data = port_stats_history[-1][dpid_from_name(link[0])][self.topo[link[0]][link[1]]['port']]
                rx_occ = 100 * port_data[0] * 8 / (self.topo[link[0]][link[1]]['bw'] * self.stats_req_time_interval * 1e6)
                tx_occ = 100 * port_data[1] * 8 / (self.topo[link[0]][link[1]]['bw'] * self.stats_req_time_interval * 1e6)
                rx_dropped, tx_dropped = port_data[2:4]
                print self.topo[link[0]][link[1]]['port'], "RX = %.2f%%, TX = %.2f%%" % (rx_occ, tx_occ)
                print self.topo[link[0]][link[1]]['port'], "RX_dropped = %d, TX_dropped = %d" % (rx_dropped, tx_dropped)
                occ = tx_occ
                #for occ in (rx_occ, tx_occ):
                if occ > self.congestion_thresh:
                    link_ep = (link[0], link[1]) if occ == tx_occ else (link[1], link[0])
                    res, all_elephants_of_saturated_links[link] = self.get_elephant_by_link(flow_stats_history, link_ep)
                    if res is not None:
                        new_forwarding_list.update(res)
                topo1[link[0]][link[1]]["C_res"] = (100 - tx_occ)/100 *\
                                                   self.topo[link[0]][link[1]]['bw'] * 1e6


        if new_forwarding_list:
            print("Reroute phase")
            new_forwarding_list = sorted(new_forwarding_list.items(), key=operator.itemgetter(1), reverse=True)
            print(new_forwarding_list)
            #print topo1.edges(data=True)
            pprint(all_elephants_of_saturated_links)
            for el in new_forwarding_list:
                s = "Detected potential elastic: %s with size %s" % (el[0], bps_to_human_string(el[1]))
                if self.filename is not None:
                    with open(self.filename, "a") as myfile:
                        myfile.write(str(datetime.datetime.now()) + '\n' + str(all_elephants_of_saturated_links) + '\n')
                green_msg(s)
                continue
                green_msg("Attempting to reroute: %s with size %s" % (el[0], bps_to_human_string(el[1])))
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
                        path_old, priority_old,_ = self.ep_path_map[elephant_ep]
                        self.update_link_ep_map(path_old, "REM")
                        self.update_capacity(path_old, topo1, "REM", elephant_size)

                    self.update_link_ep_map(path, "ADD")
                    self.update_capacity(path, topo1, "ADD", elephant_size)
                    self.apply_routing(path, topo1, priority_old+1, idle_timeout=self.time_interval_np)
                    print 'Rerouted path %s->%s' % (elephant_ep[0], elephant_ep[1]), path
                    print "Route installed with priority", priority_old+1
                    self.ep_path_map[elephant_ep] = (path, priority_old+1, get_switch_time()+self.time_interval_np*1000)
                except nx.NetworkXNoPath:
                    print "Can't reroute: No path between %s and %s" % (elephant_ep[0], elephant_ep[1])