import logging
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import ryu.ofproto.beba_v1_0 as bebaproto
import ryu.ofproto.beba_v1_0_parser as bebaparser
import struct

LOG = logging.getLogger('app.beba.sample_and_hold')
devices=[]

class BebaSampleAndHold(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(BebaSampleAndHold, self).__init__(*args, **kwargs)

    def add_flow(self, datapath, table_id, priority, match, actions):
        if len(actions) > 0:
            inst = [ofparser.OFPInstructionActions(
                ofproto.OFPIT_APPLY_ACTIONS, actions)]
        else:
            inst = []
        mod = ofparser.OFPFlowMod(datapath=datapath, table_id=table_id,
                                  priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):

        # here i set the sample interval
        sample_interval = 10
        heavy_threshold = 40

        msg = ev.msg
        datapath = msg.datapath
        devices.append(datapath)
        LOG.info("This app counts packets using sample and hold technique..." )
        LOG.info("Configuring switch %d..." % datapath.id)

        """ Set table 0 as stateful """
        req = bebaparser.OFPExpMsgConfigureStatefulTable(datapath=datapath,table_id=0,stateful=1)
        datapath.send_msg(req)

        """ Set lookup extractor = {eth_src} """
        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath,command=bebaproto.OFPSC_EXP_SET_L_EXTRACTOR,fields=[ofproto.OXM_OF_IPV4_SRC],table_id=0)
        datapath.send_msg(req)

        """ Set update extractor = {eth_src} """
        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath,command=bebaproto.OFPSC_EXP_SET_U_EXTRACTOR,fields=[ofproto.OXM_OF_IPV4_SRC],table_id=0)
        datapath.send_msg(req)

        # gd_0 is sample counter
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath,table_id=0,global_data_variable_id=0,value=1)
        datapath.send_msg(req)
        # gd_1 is sample interval
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath,table_id=0,global_data_variable_id=1,value=sample_interval)
        datapath.send_msg(req)
        # gd_1 is sample interval
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=0, global_data_variable_id=2,value=heavy_threshold)
        datapath.send_msg(req)

        # i check if the sample counter (gd_0) is equal to sample_interval (gd_1)
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=0,
                                               condition_id=0,
                                               condition=bebaproto.CONDITION_EQ,
                                               operand_1_gd_id=0,
                                               operand_2_gd_id=1)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=0,
                                               condition_id=1,
                                               condition=bebaproto.CONDITION_GTE,
                                               operand_1_fd_id=0,
                                               operand_2_gd_id=2)
        datapath.send_msg(req)


		
        #if (C0 == 0):GD0+=1
        match = ofparser.OFPMatch(state = 0,condition0=0)
        actions = [bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD )]
        self.add_flow(datapath=datapath,table_id=0,priority=0,match=match,actions=actions)

        # if (C0 == 1): GD0=1
        match = ofparser.OFPMatch(state = 0,condition0=1)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=0),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUB,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=sample_interval-1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath,table_id=0,priority=0,match=match,actions=actions)

        #if (C0 == 0) and (C1 == 0): GD0+=1, FD0+=1
        match = ofparser.OFPMatch(state = 1,condition0=0,condition1=0)
        actions = [bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=1),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_fd_id=0,
                                                          operand_1_fd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath,table_id=0,priority=0,match=match,actions=actions)

        # if (C0 == 0) and (C1 == 1): GD0+=1, FD0+=1, state=2
        match = ofparser.OFPMatch(state=1, condition0=0, condition1=1)
        actions = [bebaparser.OFPExpActionSetState(state=2, table_id=0),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=1),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_fd_id=0,
                                                          operand_1_fd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath, table_id=0, priority=0, match=match, actions=actions)

        # if (C0 == 1) and (C1 == 0): GD0=1, FD0+=1
        match = ofparser.OFPMatch(state=1, condition0=1, condition1=0)
        actions = [bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUB,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=sample_interval-1),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_fd_id=0,
                                                          operand_1_fd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath, table_id=0, priority=0, match=match, actions=actions)

        # if (C0 == 1) and (C1 == 1): GD0=1, FD0+=1, state=2
        match = ofparser.OFPMatch(state=1, condition0=1, condition1=1)
        actions = [bebaparser.OFPExpActionSetState(state=2, table_id=0),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUB,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=sample_interval-1),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_fd_id=0,
                                                          operand_1_fd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath, table_id=0, priority=0, match=match, actions=actions)

        # if (C0 == 0): GD0+=1, FD0+=1
        match = ofparser.OFPMatch(state=2, condition0=0)
        actions = [bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=1),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_fd_id=0,
                                                          operand_1_fd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath,table_id=0,priority=0,match=match,actions=actions)

    # if (C0 == 1): GD0=1, FD0+=1
        match = ofparser.OFPMatch(state=2, condition0=1)
        actions = [bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUB,
                                                          output_gd_id=0,
                                                          operand_1_gd_id=0,
                                                          operand_2_cost=sample_interval - 1),
                   bebaparser.OFPExpActionSetDataVariable(table_id=0,
                                                          opcode=bebaproto.OPCODE_SUM,
                                                          output_fd_id=0,
                                                          operand_1_fd_id=0,
                                                          operand_2_cost=1),
                   ofparser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        self.add_flow(datapath=datapath, table_id=0, priority=0, match=match, actions=actions)



    @set_ev_cls(ofp_event.EventOFPExperimenterStatsReply, MAIN_DISPATCHER)
    def packet_in_handler(self, event):
        msg = event.msg
        if (msg.body.experimenter == 0xBEBABEBA):
            if (msg.body.exp_type == bebaproto.OFPMP_EXP_GLOBAL_DATA_STATS):
                global_data_list = bebaparser.OFPGlobalDataStats.parser(msg.body.data)
                for index,global_data in enumerate(global_data_list):
                    print ('global data '+str(index)+' = ' + str(global_data.value))


import time
from threading import Thread


def ask_for_state(t):

    #Query every t seconds for the global data variables


    while True:
        time.sleep(t)
        if devices == []:
            print ("No connected device")
        else:
            m = bebaparser.OFPExpGlobalDataStatsMultipartRequest(devices[0], table_id=0)
            devices[0].send_msg(m)
            print('*' * 30)
            print("GlobalDataStatsMultipartRequest sent")



t = Thread(target=ask_for_state, args=((5,)))
t.start()
