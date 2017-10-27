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
from utils import get_switch_time


class BebaSampleAndHold(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(BebaSampleAndHold, self).__init__(*args, **kwargs)
        self.sample_interval = 1
        self.time_interval = 10
        self.hh_threshold = 500000
        self.congestion_thresh = 95
        self.map_proto_tableid = {"tcp": 2, "udp": 3}
        self.ip_proto_values = {'tcp': 6, 'udp': 17}

        self.counter_rv_flow_stats = 0
        self.counter_rv_port_stats = 0
        self.devices = dict()
        self.port_stats = dict()
        self.port_stats_history = list()
        self.flow_stats_history = list()

        self.Routing = routing.Routing(self.devices, table_id=0, next_table_id=1, time_interval=self.time_interval)

        self.SampleAndHold = sample_and_hold.SampleAndHold(table_id=1, map_proto_tableid=self.map_proto_tableid,
                                                           ip_proto_values=self.ip_proto_values,
                                                           sample_interval=self.sample_interval)

        self.CountTCPflows = flows_counter.FlowsCounter(table_id = self.map_proto_tableid["tcp"],
                                                        ip_proto = self.ip_proto_values["tcp"],
                                                        time_interval = self.time_interval-0.1,
                                                        hh_threshold = self.hh_threshold)

        self.CountUDPflows = flows_counter.FlowsCounter(table_id=self.map_proto_tableid["udp"],
                                                        ip_proto=self.ip_proto_values["udp"],
                                                        time_interval=self.time_interval-0.1,
                                                        hh_threshold=self.hh_threshold)

        self.log = logging.getLogger('app.beba.sample_and_hold')
        hub.spawn(self.monitor)

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

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, event):
        dpid = event.msg.datapath.id
        for stat in event.msg.body:
            if stat.port_no != ofproto.OFPP_LOCAL:
                self.port_stats_history[-1].setdefault(dpid,dict())
                self.port_stats_history[-1][dpid][stat.port_no] = (stat.rx_bytes - self.port_stats.get((dpid,stat.port_no),(0,0))[0],
                                                                   stat.tx_bytes - self.port_stats.get((dpid,stat.port_no),(0,0))[1])
                self.port_stats[(dpid,stat.port_no)] = (stat.rx_bytes, stat.tx_bytes)
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
            hub.sleep(self.time_interval)