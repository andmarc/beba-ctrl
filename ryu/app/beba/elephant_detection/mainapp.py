import logging
import re

import flows_counter
import routing
import sample_and_hold
import ryu.ofproto.beba_v1_0 as bebaproto
import ryu.ofproto.beba_v1_0_parser as bebaparser
import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from utils import get_switch_time, bps_to_human_string, green_msg, red_msg
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
import json
from webob import Response
from networkx.readwrite import json_graph
import threading

import os

sample_and_hold_instance_name = 'sample_and_hold_api_app'
url = '/sample_and_hold'


class BebaSampleAndHold(app_manager.RyuApp):

    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(BebaSampleAndHold, self).__init__(*args, **kwargs)
        self.sample_interval = 1
        self.stats_req_time_interval = 10
        self.switch_window_time_interval = self.stats_req_time_interval - 0.1
        self.hh_threshold = 500000
        self.congestion_thresh = 90
        self.map_proto_tableid = {"tcp": 2, "udp": 3}
        self.ip_proto_values = {'tcp': 6, 'udp': 17}

        self.counter_rv_flow_stats = 0
        self.counter_rv_port_stats = 0
        self.devices = dict()
        self.port_stats = dict()
        self.port_stats_history = list()
        self.flow_stats_history = list()

        self.Routing = routing.Routing(self.devices, table_id=0, next_table_id=1, stats_req_time_interval=self.stats_req_time_interval,
                                       switch_window_time_interval=self.switch_window_time_interval, congestion_thresh=self.congestion_thresh,
                                       filename=os.environ['FILENAME'] if 'FILENAME' in os.environ else None)

        self.SampleAndHold = sample_and_hold.SampleAndHold(table_id=1, map_proto_tableid=self.map_proto_tableid,
                                                           ip_proto_values=self.ip_proto_values,
                                                           sample_interval=self.sample_interval)

        self.CountTCPflows = flows_counter.FlowsCounter(table_id = self.map_proto_tableid["tcp"],
                                                        ip_proto = self.ip_proto_values["tcp"],
                                                        time_interval = self.switch_window_time_interval,
                                                        hh_threshold = self.hh_threshold)

        self.CountUDPflows = flows_counter.FlowsCounter(table_id=self.map_proto_tableid["udp"],
                                                        ip_proto=self.ip_proto_values["udp"],
                                                        time_interval=self.switch_window_time_interval,
                                                        hh_threshold=self.hh_threshold)

        self.log = logging.getLogger('app.beba.sample_and_hold')
        hub.spawn(self.monitor)

        # TM monitoring (HEAVY!!!) #####################################################################################
        self.flow_stats_req_time_interval = 5

        self.latest_per_switch_flow_stats = {}  # latest per-switch per-flow stats (match, byte_count, duration_sec)
        self.latest_per_switch_flow_rate = {}  # latest per-switch per-flow rate (bps)
        # NB: we keep both min and max to capture pkt drops due to a bottleneck!
        self.latest_flow_rate_min = {}  # aggregated minimum (among all switches) per-flow rate (bps)
        self.latest_flow_rate_max = {}  # aggregated maximum (among all switches) per-flow rate (bps)
        self.flow_rate_min_max_history = []  # history of aggregated min/max per-flow rate for each measurement round
        # we define the same set of data structure reserved for rerouted flows
        self.latest_per_switch_flow_stats_R = {}
        self.latest_per_switch_flow_rate_R = {}
        self.latest_flow_rate_min_R = {}
        self.latest_flow_rate_max_R = {}
        self.flow_rate_min_max_history_R = []
        hub.spawn(self.monitor_all_flows_rate)
        self.lock = threading.Lock()
        ################################################################################################################

        # REST API
        wsgi = kwargs['wsgi']
        wsgi.register(SampleAndHoldRestController, {sample_and_hold_instance_name: self})

########################################################################################################################

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        self.devices[datapath.id] = datapath
        self.log.info("This app counts packets using sample and hold technique..." )
        self.log.info("Configuring switch %d..." % datapath.id)

        self.SampleAndHold.configure_stateful_table(datapath)
        self.CountTCPflows.configure_double_stateful_table(datapath)
        self.CountUDPflows.configure_double_stateful_table(datapath)
        self.Routing.initialize_routing()
        self.latest_per_switch_flow_stats[datapath.id] = {}
        self.latest_per_switch_flow_stats_R[datapath.id] = {}
        self.latest_per_switch_flow_rate[datapath.id] = {}
        self.latest_per_switch_flow_rate_R[datapath.id] = {}

########################################################################################################################

    def check_stats_rv(self):
        if self.counter_rv_port_stats == len(self.devices) and self.counter_rv_flow_stats == 2*len(self.devices):
            #print(self.port_stats_history, self.flow_stats_history)
            self.log.info("Stats response received")
            self.Routing.react(self.port_stats_history, self.flow_stats_history, self.starts_ts)

    # State Sync: Parse the response
    @set_ev_cls(ofp_event.EventOFPExperimenterStatsReply, MAIN_DISPATCHER)
    def packet_in_handler(self, event):
        msg = event.msg
        dpid = msg.datapath.id

        if msg.body.experimenter == 0xBEBABEBA:
            if msg.body.exp_type == bebaproto.OFPMP_EXP_STATE_STATS:
                state_entry_list = bebaparser.OFPStateStats.parser(msg.body.data)
                if not state_entry_list:
                    #self.log.info("No key for this state")
                    pass
                else:
                    self.flow_stats_history[-1].setdefault(dpid, dict())
                    flows_extracted = dict()
                    for state_entry in state_entry_list:
                        #table_id = state_entry.table_id
                        flow_label_string = bebaparser.state_entry_key_to_str(state_entry)
                        flow_label = re.findall('\"(.*?)\"',flow_label_string)
                        flow_label.insert(2,'T' if 'tcp' in flow_label_string else 'U')
                        flow_label[3]=int(flow_label[3])
                        flow_label[4]=int(flow_label[4])
                        flows_extracted[tuple(flow_label)]= state_entry.entry.flow_data_var[:4]
                    self.flow_stats_history[-1][dpid].update(flows_extracted)
                    #self.flow_stats_history[-1][dpid][table_id] = flows_extracted
                    #print(self.flow_stats_history[-1][dpid])
                    #self.log.info("switch n", dpid, self.flow_stats_history[-1][dpid], "\n")
                self.counter_rv_flow_stats += 1
            self.check_stats_rv()

    @staticmethod
    def match_str(flow_stat_match):
        # This should be a switch-independent flow definition (i.e. we do not include the match on the in_port to be
        # able to aggregate different measurements of the same flow from different switches)
        # TODO is this inefficient?
        return flow_stat_match['ipv4_src'] + (':' + flow_stat_match['tcp_src'] if 'tcp_src' in flow_stat_match else '')\
               + '-' + \
               flow_stat_match['ipv4_dst'] + (':' + flow_stat_match['tcp_dst'] if 'tcp_dst' in flow_stat_match else '')

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        if len(ev.msg.body) == 0:
            return
        dp_id = ev.msg.datapath.id
        self.latest_per_switch_flow_rate[dp_id] = {}
        self.latest_per_switch_flow_rate_R[dp_id] = {}

        for flow_stat in ev.msg.body:
            if flow_stat.priority == 0:
                if self.match_str(flow_stat.match) in self.latest_per_switch_flow_stats[dp_id]:

                    old_flow_stat = self.latest_per_switch_flow_stats[dp_id][self.match_str(flow_stat.match)]
                    old_byte_count = old_flow_stat.byte_count
                    old_duration_sec = old_flow_stat.duration_sec

                    self.latest_per_switch_flow_stats[dp_id][self.match_str(flow_stat.match)] = flow_stat

                    delta_bit = (flow_stat.byte_count - old_byte_count)*8
                    delta_duration = flow_stat.duration_sec - old_duration_sec

                    if delta_duration > 0:
                        self.latest_per_switch_flow_rate[dp_id][self.match_str(flow_stat.match)] = 1.0*delta_bit/delta_duration
                else:
                    self.latest_per_switch_flow_stats[dp_id][self.match_str(flow_stat.match)] = flow_stat
            else:
                # Flows with non-zero priority are re-routed flows
                if self.match_str(flow_stat.match) in self.latest_per_switch_flow_stats_R[dp_id]:

                    old_flow_stat = self.latest_per_switch_flow_stats_R[dp_id][self.match_str(flow_stat.match)]
                    old_byte_count = old_flow_stat.byte_count
                    old_duration_sec = old_flow_stat.duration_sec

                    self.latest_per_switch_flow_stats_R[dp_id][self.match_str(flow_stat.match)] = flow_stat

                    delta_bit = (flow_stat.byte_count - old_byte_count)*8
                    delta_duration = flow_stat.duration_sec - old_duration_sec

                    if delta_duration > 0:
                        self.latest_per_switch_flow_rate_R[dp_id][self.match_str(flow_stat.match)] = 1.0*delta_bit/delta_duration
                else:
                    self.latest_per_switch_flow_stats_R[dp_id][self.match_str(flow_stat.match)] = flow_stat

        if len(self.latest_per_switch_flow_rate[dp_id]) > 0:
            # green_msg('TM from datapath %d' % dp_id)
            for flow in self.latest_per_switch_flow_rate[dp_id]:
                # print flow, bps_to_human_string(self.latest_per_switch_flow_rate[dp_id][flow])
                with self.lock:
                    if flow not in self.latest_flow_rate_max:
                        self.latest_flow_rate_max[flow] = self.latest_per_switch_flow_rate[dp_id][flow]
                    else:
                        # The global TM contains the worst case rate of the current measurement round
                        self.latest_flow_rate_max[flow] = max(self.latest_per_switch_flow_rate[dp_id][flow], self.latest_flow_rate_max[flow])
                with self.lock:
                    if flow not in self.latest_flow_rate_min:
                        self.latest_flow_rate_min[flow] = self.latest_per_switch_flow_rate[dp_id][flow]
                    else:
                        # The global TM contains the worst case rate of the current measurement round
                        self.latest_flow_rate_min[flow] = min(self.latest_per_switch_flow_rate[dp_id][flow], self.latest_flow_rate_min[flow])
            # print

        if len(self.latest_per_switch_flow_rate_R[dp_id]) > 0:
            # green_msg('TM from datapath %d' % dp_id)
            for flow in self.latest_per_switch_flow_rate_R[dp_id]:
                # print flow, bps_to_human_string(self.latest_per_switch_flow_rate_R[dp_id][flow])
                with self.lock:
                    if flow not in self.latest_flow_rate_max_R:
                        self.latest_flow_rate_max_R[flow] = self.latest_per_switch_flow_rate_R[dp_id][flow]
                    else:
                        # The global TM contains the worst case rate of the current measurement round
                        self.latest_flow_rate_max_R[flow] = max(self.latest_per_switch_flow_rate_R[dp_id][flow], self.latest_flow_rate_max_R[flow])
                with self.lock:
                    if flow not in self.latest_flow_rate_min_R:
                        self.latest_flow_rate_min_R[flow] = self.latest_per_switch_flow_rate_R[dp_id][flow]
                    else:
                        # The global TM contains the worst case rate of the current measurement round
                        self.latest_flow_rate_min_R[flow] = min(self.latest_per_switch_flow_rate_R[dp_id][flow], self.latest_flow_rate_min_R[flow])
            # print

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, event):
        dpid = event.msg.datapath.id
        for stat in event.msg.body:
            if stat.port_no != ofproto.OFPP_LOCAL:
                self.port_stats_history[-1].setdefault(dpid,dict())
                self.port_stats_history[-1][dpid][stat.port_no] = (stat.rx_bytes - self.port_stats.get((dpid,stat.port_no),(0,0,0,0))[0],
                                                                   stat.tx_bytes - self.port_stats.get((dpid,stat.port_no),(0,0,0,0))[1],
                                                                   stat.rx_dropped - self.port_stats.get((dpid, stat.port_no), (0,0,0,0))[2],
                                                                   stat.tx_dropped - self.port_stats.get((dpid, stat.port_no), (0,0,0,0))[3])
                self.port_stats[(dpid,stat.port_no)] = (stat.rx_bytes, stat.tx_bytes, stat.rx_dropped, stat.tx_dropped)
        self.counter_rv_port_stats += 1
        self.check_stats_rv()

########################################################################################################################

    def ask_for_state(self, device, table_id, state):
        m = bebaparser.OFPExpGetFlowsInState(device, table_id= table_id, state=state)
        device.send_msg(m)
        #self.log.info("GetFlowsInState message sent")

    def ask_for_port_stats(self, device):
        m = ofparser.OFPPortStatsRequest(device, 0, ofproto.OFPP_ANY)
        device.send_msg(m)
        #self.log.info("OFPPortStatsRequest sent")

    def ask_for_flow_stats(self, device, table_id=0):
        m = ofparser.OFPFlowStatsRequest(device, table_id=table_id)
        device.send_msg(m)

########################################################################################################################

    def monitor(self):
        # "state 1":"monitoring", "state 2":"elephant"
        state = 2
        self.starts_ts = get_switch_time()
        while True:
            self.counter_rv_flow_stats = 0
            self.counter_rv_port_stats = 0
            self.log.info("*" * 40 + "\n")
            if not self.devices:
                self.log.info("No connected device")
            else:
                self.port_stats_history.append(dict())
                self.flow_stats_history.append(dict())
                self.log.info("StatsRequest sent")
                for device in self.devices.values():
                    self.ask_for_port_stats(device)
                    for table_id in self.map_proto_tableid.values():
                        self.ask_for_state(device, table_id, state)
            hub.sleep(self.stats_req_time_interval)

    def monitor_all_flows_rate(self):
        while True:
            if not self.devices:
                self.log.info("No connected device")
            else:
                if min(len(self.latest_flow_rate_min), len(self.latest_flow_rate_max)) > 0:
                    # Here we plot an 'old' global TM to be sure that it results from ALL the local TMs
                    red_msg('Last global TM (related to 1 flow_stats_req_time_interval ago)')
                    d = {}
                    for flow in self.latest_flow_rate_min:
                        if min(self.latest_flow_rate_min[flow], self.latest_flow_rate_max[flow]) > 0:
                            print flow, ':', bps_to_human_string(self.latest_flow_rate_min[flow]), '~', bps_to_human_string(self.latest_flow_rate_max[flow])
                        d[flow] = (self.latest_flow_rate_min[flow], self.latest_flow_rate_max[flow])
                    self.flow_rate_min_max_history.append(d)
                    # We make space also for the history of rerouted flow to make them aligned in time
                    self.flow_rate_min_max_history_R.append({})
                    print

                if min(len(self.latest_flow_rate_min_R), len(self.latest_flow_rate_max_R)) > 0:
                    # Here we plot an 'old' global TM to be sure that it results from ALL the local TMs
                    red_msg('Last global TM of rerouted flows (related to 1 flow_stats_req_time_interval ago)')
                    d = {}
                    for flow in self.latest_flow_rate_min_R:
                        if min(self.latest_flow_rate_min_R[flow], self.latest_flow_rate_max_R[flow]) > 0:
                            print flow, ':', bps_to_human_string(self.latest_flow_rate_min_R[flow]), '~', bps_to_human_string(self.latest_flow_rate_max_R[flow])
                        d[flow] = (self.latest_flow_rate_min_R[flow], self.latest_flow_rate_max_R[flow])
                    self.flow_rate_min_max_history_R[-1].update(d)
                    print

                self.log.info("FlowStatsRequest sent")
                with self.lock:
                    self.latest_flow_rate_min = {}
                    self.latest_flow_rate_max = {}
                    self.latest_flow_rate_min_R = {}
                    self.latest_flow_rate_max_R = {}
                for device_id, device in self.devices.items():
                    self.latest_per_switch_flow_rate[device_id] = {}
                    self.latest_per_switch_flow_rate_R[device_id] = {}
                    self.ask_for_flow_stats(device)
            hub.sleep(self.flow_stats_req_time_interval)

class SampleAndHoldRestController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(SampleAndHoldRestController, self).__init__(req, link, data, **config)
        self.sample_and_hold_app = data[sample_and_hold_instance_name]

    @route('sample_and_hold', url + '/topo', methods=['GET'])
    def get_topo(self, req, **kwargs):
        topo = self.sample_and_hold_app.Routing.topo
        # TODO
        # topo = residual(topo)
        body = json.dumps(json_graph.node_link_data(topo))
        return Response(content_type='application/json', body=body)
        # return Response(status=404)

    @route('sample_and_hold', url + '/flow_rate_min_max_history', methods=['GET'])
    def get_flow_rate_min_max_history(self, req, **kwargs):
        body = json.dumps(self.sample_and_hold_app.flow_rate_min_max_history)
        return Response(content_type='application/json', body=body)
        # return Response(status=404)

    @route('sample_and_hold', url + '/flow_rate_min_max_history_R', methods=['GET'])
    def get_flow_rate_min_max_history_R(self, req, **kwargs):
        body = json.dumps(self.sample_and_hold_app.flow_rate_min_max_history_R)
        return Response(content_type='application/json', body=body)
        # return Response(status=404)
