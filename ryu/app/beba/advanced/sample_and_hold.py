import logging
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER,MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import ryu.ofproto.beba_v1_0 as bebaproto
import ryu.ofproto.beba_v1_0_parser as bebaparser

LOG = logging.getLogger('app.beba.sample_and_hold')
devices = []

debug_on = False
overflow_prevention = False


class BebaSampleAndHold(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(BebaSampleAndHold, self).__init__(*args, **kwargs)
        self.sample_interval = 1
        self.time_interval = 12*1000
        self.hh_threshold = 19750618*0.01
        self.port = ofproto.OFPP_FLOOD

    @staticmethod
    def add_flow(datapath, table_id, priority, match, actions, inst2=None):
        actions = filter(lambda x: x is not None, actions)
        if len(actions) > 0:
            inst = [ofparser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        else:
            inst = []
        if inst2 is not None:
            inst += inst2
        mod = ofparser.OFPFlowMod(datapath=datapath, table_id=table_id,
                                  priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    def configure_double_stateful_table(self,datapath,table_id):

        assert (table_id == 1 or table_id == 2)

        req = bebaparser.OFPExpMsgConfigureStatefulTable(datapath=datapath, table_id=table_id, stateful=1)
        datapath.send_msg(req)

########################################################################################################################

        if table_id == 1:
            # lookup for TCP TABLE
            fields_proto = [ofproto.OXM_OF_IPV4_SRC, ofproto.OXM_OF_IPV4_DST,ofproto.OXM_OF_TCP_SRC, ofproto.OXM_OF_TCP_DST]
        else:
            # lookup for  UDP TABLE
            fields_proto = [ofproto.OXM_OF_IPV4_SRC, ofproto.OXM_OF_IPV4_DST,ofproto.OXM_OF_UDP_SRC, ofproto.OXM_OF_UDP_DST]

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath, command=bebaproto.OFPSC_EXP_SET_L_EXTRACTOR,
                                             fields=fields_proto, table_id=table_id)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath, command=bebaproto.OFPSC_EXP_SET_U_EXTRACTOR,
                                             fields=fields_proto, table_id=table_id)
        datapath.send_msg(req)

########################################################################################################################

        # HF[0] = OXM_EXP_TIMESTAMP [ms]
        req = bebaparser.OFPExpMsgHeaderFieldExtract(
            datapath=datapath,
            table_id=table_id,
            extractor_id=0,
            field=bebaproto.OXM_EXP_TIMESTAMP
        )
        datapath.send_msg(req)

        # HF[1] = OXM_EXP_PKT_LEN
        req = bebaparser.OFPExpMsgHeaderFieldExtract(
            datapath=datapath,
            table_id=table_id,
            extractor_id=1,
            field=bebaproto.OXM_EXP_PKT_LEN
        )
        datapath.send_msg(req)

########################################################################################################################

        # GD[0] is time interval[ms]
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=table_id, global_data_variable_id=0,
                                                         value=self.time_interval)
        datapath.send_msg(req)

        # GD[1] is heavy hitter threshold [byte]
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=table_id, global_data_variable_id=1,
                                                         value=self.hh_threshold)
        datapath.send_msg(req)

########################################################################################################################

        # C0: check if the packet timestamp (HF[0]) is >= to end of time interval (FD[0])
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=table_id,
                                               condition_id=0,
                                               condition=bebaproto.CONDITION_GTE,
                                               operand_1_hf_id=0,
                                               operand_2_fd_id=0)
        datapath.send_msg(req)

        # C1: check if the byte counted (FD[1]) are >= to heavy hitter threshold (GD[1])
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=table_id,
                                               condition_id=1,
                                               condition=bebaproto.CONDITION_GTE,
                                               operand_1_fd_id=1,
                                               operand_2_gd_id=1)
        datapath.send_msg(req)

########################################################################################################################

        # FD[0]= GD[0] + HF[0]
        reset_time_interval_action = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                                            opcode=bebaproto.OPCODE_SUM,
                                                                            output_fd_id=0,
                                                                            operand_1_gd_id=0,
                                                                            operand_2_hf_id=0)

        # FD[1]= HF[1] + 0 = HF[1]
        init_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                                     opcode=bebaproto.OPCODE_SUM,
                                                                     output_fd_id=1,
                                                                     operand_1_hf_id=1,
                                                                     operand_2_cost=0)

        # FD[1]+= HF[1]
        increment_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                                          opcode=bebaproto.OPCODE_SUM,
                                                                          output_fd_id=1,
                                                                          operand_1_fd_id=1,
                                                                          operand_2_hf_id=1)

########################################################################################################################
        ''' actions enabled if dbg_on == True, used only for debugging '''

        # FD[2]= FD[0]
        save_ts = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                         opcode=bebaproto.OPCODE_SUM,
                                                         output_fd_id=2,
                                                         operand_1_fd_id=0,
                                                         operand_2_cost=0) if debug_on else None

        # FD[3]= FD[1]
        save_counter = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                              opcode=bebaproto.OPCODE_SUM,
                                                              output_fd_id=3,
                                                              operand_1_fd_id=1,
                                                              operand_2_cost=0) if debug_on else None

########################################################################################################################

        # if STATE 0 and metadata == 0 : do nothing
        match = ofparser.OFPMatch(state=0, metadata=0)
        actions = []
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 0 and metadata == 1 : FD[0]=GD[0]+HF[0], FD[1]=HF[1] ,SET_STATE(1)
        match = ofparser.OFPMatch(state=0, metadata=1)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=table_id),
                   reset_time_interval_action,
                   init_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

########################################################################################################################

        # if STATE 1 and  C0 and  C1:  FD[0]=GD[0]+HF[0], FD[1]=HF1 , SET_STATE(2)
        match = ofparser.OFPMatch(state=1, condition0=1, condition1=1)
        actions = [bebaparser.OFPExpActionSetState(state=2, table_id=table_id),
                   save_counter,
                   save_ts,
                   reset_time_interval_action,
                   init_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 1 and  C0 and  NOT C1:  FD[0]=GD[0]+HF[0], FD[1]=HF1
        match = ofparser.OFPMatch(state=1, condition0=1, condition1=0)
        actions = [save_counter,
                   save_ts,
                   reset_time_interval_action,
                   init_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 1 and  NOT C0 and  NOT C1:  FD[1]+=HF1
        match = ofparser.OFPMatch(state=1, condition0=0, condition1=0)
        actions = [increment_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 1 and  NOT C0 and  C1:  do nothing (overflow prevention)
        match = ofparser.OFPMatch(state=1, condition0=0, condition1=1)
        actions = [None if overflow_prevention else increment_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

########################################################################################################################

        # if STATE 2 and  C0 and  C1:  FD[0]=GD[0]+HF[0], FD[1]=HF1
        match = ofparser.OFPMatch(state=2, condition0=1, condition1=1)
        actions = [save_counter,
                   save_ts,
                   reset_time_interval_action,
                   init_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 2 and  C0 and  NOT C1:  FD[0]=GD[0]+HF[0], FD[1]=HF1, , SET_STATE(1)
        match = ofparser.OFPMatch(state=2, condition0=1, condition1=0)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=table_id),
                   save_counter,
                   save_ts,
                   reset_time_interval_action,
                   init_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 2 and  NOT C0 and  NOT C1:  FD[1]+=HF1
        match = ofparser.OFPMatch(state=2, condition0=0, condition1=0)
        actions = [increment_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

        # if STATE 2 and  NOT C0 and  C1:  do nothing (overflow prevention)
        match = ofparser.OFPMatch(state=2, condition0=0, condition1=1)
        actions = [None if overflow_prevention else increment_counter_action]
        self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

########################################################################################################################

    def configure_first_stateful_table(self,datapath,table_id):
        assert (table_id == 0)
        req = bebaparser.OFPExpMsgConfigureStatefulTable(datapath=datapath, table_id=table_id, stateful=1)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath,
                                             command=bebaproto.OFPSC_EXP_SET_L_EXTRACTOR,
                                             fields=[ofproto.OXM_OF_ETH_SRC],
                                             table_id=0)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath,
                                             command=bebaproto.OFPSC_EXP_SET_U_EXTRACTOR,
                                             fields=[ofproto.OXM_OF_ETH_SRC],
                                             table_id=0)
        datapath.send_msg(req)

########################################################################################################################

        # GD[0] is sample counter
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=table_id, global_data_variable_id=0,
                                                         value=self.sample_interval-1)
        datapath.send_msg(req)

        # GD[1] is sample interval
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=table_id, global_data_variable_id=1,
                                                         value=self.sample_interval-1)
        datapath.send_msg(req)

########################################################################################################################

        # C0: check if the sample counter (GD[0]) is equal to sample_interval (GD[1])
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=table_id,
                                               condition_id=0,
                                               condition=bebaproto.CONDITION_EQ,
                                               operand_1_gd_id=0,
                                               operand_2_gd_id=1)
        datapath.send_msg(req)

########################################################################################################################

        #GD[0]+=1
        increment_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                                          opcode=bebaproto.OPCODE_SUM,
                                                                          output_gd_id=0,
                                                                          operand_1_gd_id=0,
                                                                          operand_2_cost=1)

        #GD[0]=0
        reset_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=table_id,
                                                                      opcode=bebaproto.OPCODE_SUB,
                                                                      output_gd_id=0,
                                                                      operand_1_gd_id=0,
                                                                      operand_2_gd_id=0)

########################################################################################################################

        ip_proto_values = {'tcp': 6, 'udp': 17}
        proto_table_map = {'tcp': 1, 'udp': 2}

        """if is_cond: reset_counter_action else: increment_counter_action"""
        for is_cond, action in enumerate([increment_counter_action,reset_counter_action]):
            """Add sample metadata (0/1) and send to TCP/UDP stateful table"""
            for i in ip_proto_values:
                    match = ofparser.OFPMatch(eth_type=0x0800, ip_proto=ip_proto_values[i], condition0=is_cond)
                    self.add_flow(datapath=datapath, table_id=table_id, priority=10, match=match,
                                  actions = [action,ofparser.OFPActionOutput(self.port)],
                                  inst2=[ofparser.OFPInstructionGotoTable(proto_table_map[i]),
                                         ofparser.OFPInstructionWriteMetadata(is_cond, 0x1)])

            """Match all the packet not matched above"""
            match = ofparser.OFPMatch(condition0=is_cond)
            actions = [action,ofparser.OFPActionOutput(self.port)]
            self.add_flow(datapath=datapath, table_id=table_id, priority=0, match=match, actions=actions)

########################################################################################################################

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        devices.append(datapath)
        LOG.info("This app counts packets using sample and hold technique..." )
        LOG.info("Configuring switch %d..." % datapath.id)
        """Table0: stateful, it's used to sample and split traffic between table1 (TCP) and table2 (UDP)"""
        for table_id in range(3):
            if not table_id:
                self.configure_first_stateful_table(datapath,table_id)
            else:
                self.configure_double_stateful_table(datapath,table_id)

########################################################################################################################

    # State Sync: Parse the response
    @set_ev_cls(ofp_event.EventOFPExperimenterStatsReply, MAIN_DISPATCHER)
    def packet_in_handler(self, event):
        msg = event.msg

        if msg.body.experimenter == 0xBEBABEBA:
            if msg.body.exp_type == bebaproto.OFPMP_EXP_STATE_STATS:
                state_entry_list = bebaparser.OFPStateStats.parser(msg.body.data)
                if state_entry_list == []:
                    print "No key for this state"
                else:
                    for state_entry in state_entry_list:
                        print 'State :', state_entry.entry.state
                        print 'Key   :', bebaparser.state_entry_key_to_str(state_entry)
                        print 'FDV   :', state_entry.entry.flow_data_var
                        print '*********'

########################################################################################################################

import time
from threading import Thread

def ask_for_state(t, state):
    """State Sync: Get the flows in a given state"""

    counter = 0
    while True:
        time.sleep(t)
        if devices == []:
            print ("No connected device")
        else:

            m = bebaparser.OFPExpGetFlowsInState(devices[0], table_id=1, state=state)
            devices[0].send_msg(m)
            print("GetFlowsInState message sent" + str(counter))
        counter += 1

state = 2
t = Thread(target=ask_for_state, args=(12, state))
t.start()