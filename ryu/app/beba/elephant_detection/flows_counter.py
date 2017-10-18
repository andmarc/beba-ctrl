import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import ryu.ofproto.beba_v1_0 as bebaproto
import ryu.ofproto.beba_v1_0_parser as bebaparser
from utils import add_flow


class FlowsCounter:
    def __init__(self, table_id, ip_proto, time_interval, hh_threshold, overflow_prevention = False, debug_on = True):
        self.table_id = table_id
        self.ip_proto = ip_proto
        self.time_interval = time_interval
        self.hh_threshold = hh_threshold
        self.overflow_prevention = overflow_prevention
        self.debug_on = debug_on

    def configure_double_stateful_table(self, datapath):
        req = bebaparser.OFPExpMsgConfigureStatefulTable(datapath=datapath, table_id=self.table_id, stateful=1)
        datapath.send_msg(req)

########################################################################################################################

        if self.ip_proto == 6:
            # lookup for TCP TABLE
            fields_proto = [ofproto.OXM_OF_IPV4_SRC, ofproto.OXM_OF_IPV4_DST,ofproto.OXM_OF_TCP_SRC, ofproto.OXM_OF_TCP_DST]
        elif self.ip_proto == 17:
            # lookup for  UDP TABLE
            fields_proto = [ofproto.OXM_OF_IPV4_SRC, ofproto.OXM_OF_IPV4_DST,ofproto.OXM_OF_UDP_SRC, ofproto.OXM_OF_UDP_DST]
        else:
            raise(Exception, "ip_proto != TCP/UDP")

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath, command=bebaproto.OFPSC_EXP_SET_L_EXTRACTOR,
                                             fields=fields_proto, table_id=self.table_id)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath, command=bebaproto.OFPSC_EXP_SET_U_EXTRACTOR,
                                             fields=fields_proto, table_id=self.table_id)
        datapath.send_msg(req)

########################################################################################################################

        # HF[0] = OXM_EXP_TIMESTAMP [ms]
        req = bebaparser.OFPExpMsgHeaderFieldExtract(
            datapath=datapath,
            table_id=self.table_id,
            extractor_id=0,
            field=bebaproto.OXM_EXP_TIMESTAMP
        )
        datapath.send_msg(req)

        # HF[1] = OXM_EXP_PKT_LEN
        req = bebaparser.OFPExpMsgHeaderFieldExtract(
            datapath=datapath,
            table_id=self.table_id,
            extractor_id=1,
            field=bebaproto.OXM_EXP_PKT_LEN
        )
        datapath.send_msg(req)

########################################################################################################################

        # GD[0] is time interval[ms]
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=self.table_id, global_data_variable_id=0,
                                                         value=self.time_interval*1000)
        datapath.send_msg(req)

        # GD[1] is heavy hitter threshold [byte]
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=self.table_id, global_data_variable_id=1,
                                                         value=self.hh_threshold)
        datapath.send_msg(req)

        # GD[2] is global end_interval [ms]
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=self.table_id, global_data_variable_id=2,
                                                         value=0)
        datapath.send_msg(req)



########################################################################################################################

        # C0: check if the packet timestamp (HF[0]) is >= to end of time interval (FD[0])
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=self.table_id,
                                               condition_id=0,
                                               condition=bebaproto.CONDITION_GTE,
                                               operand_1_hf_id=0,
                                               operand_2_gd_id=2)
        datapath.send_msg(req)

        # C1: compare the local end interval FD[0] with global end interval GD[2] to see if it is expired
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=self.table_id,
                                               condition_id=1,
                                               condition=bebaproto.CONDITION_NEQ,
                                               operand_1_fd_id=0,
                                               operand_2_gd_id=2)
        datapath.send_msg(req)

        # C2: check if the byte counted (FD[1]) are >= to heavy hitter threshold (GD[1])
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=self.table_id,
                                               condition_id=2,
                                               condition=bebaproto.CONDITION_GTE,
                                               operand_1_fd_id=1,
                                               operand_2_gd_id=1)
        datapath.send_msg(req)

########################################################################################################################

        # GD[2]= GD[0] + HF[0]
        update_global_endtime__action = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                                            opcode=bebaproto.OPCODE_SUM,
                                                                            output_gd_id=2,
                                                                            operand_1_gd_id=0,
                                                                            operand_2_hf_id=0)
        # FD[0]= GD[2] + 0
        update_local_endtime__action = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                                            opcode=bebaproto.OPCODE_SUM,
                                                                            output_fd_id=0,
                                                                            operand_1_gd_id=2,
                                                                            operand_2_cost=0)

        # FD[1]= HF[1] + 0 = HF[1]
        init_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                                     opcode=bebaproto.OPCODE_SUM,
                                                                     output_fd_id=1,
                                                                     operand_1_hf_id=1,
                                                                     operand_2_cost=0)

        # FD[1]+= HF[1]
        increment_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                                          opcode=bebaproto.OPCODE_SUM,
                                                                          output_fd_id=1,
                                                                          operand_1_fd_id=1,
                                                                          operand_2_hf_id=1)

########################################################################################################################
        ''' actions enabled if dbg_on == True, used only for debugging '''

        # FD[2]= FD[0]
        save_ts = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                         opcode=bebaproto.OPCODE_SUM,
                                                         output_fd_id=2,
                                                         operand_1_fd_id=0,
                                                         operand_2_cost=0) if self.debug_on else None

        # FD[3]= FD[1]
        save_counter = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                              opcode=bebaproto.OPCODE_SUM,
                                                              output_fd_id=3,
                                                              operand_1_fd_id=1,
                                                              operand_2_cost=0) if self.debug_on else None

########################################################################################################################

        match = ofparser.OFPMatch(state=0, metadata=0,condition0=0)
        actions = []
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=0, metadata=0,condition0=1)
        actions = [update_global_endtime__action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=0, metadata=1,condition0=0)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=self.table_id),
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=0, metadata=1,condition0=1)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=self.table_id),
                   update_global_endtime__action,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

########################################################################################################################

        match = ofparser.OFPMatch(state=1, condition0=0, condition1=1,condition2=0)
        actions = [save_ts,
                   save_counter,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=1, condition0=0, condition1=1,condition2=1)
        actions = [bebaparser.OFPExpActionSetState(state=2, table_id=self.table_id),
                   save_ts,
                   save_counter,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=1, condition0=0, condition1=0,condition2=0)
        actions = [increment_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=1, condition0=0, condition1=0, condition2=1)
        actions = [None if self.overflow_prevention else increment_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=1, condition0=1,condition2=0)
        actions = [save_ts,
                   save_counter,
                   update_global_endtime__action,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=1, condition0=1, condition2=1)
        actions = [bebaparser.OFPExpActionSetState(state=2, table_id=self.table_id),
                   save_ts,
                   save_counter,
                   update_global_endtime__action,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

########################################################################################################################

        match = ofparser.OFPMatch(state=2, condition0=0, condition1=1,condition2=0)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=self.table_id),
                   save_ts,
                   save_counter,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=2, condition0=0, condition1=1,condition2=1)
        actions = [save_ts,
                   save_counter,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=2, condition0=0, condition1=0,condition2=0)
        actions = [increment_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=2, condition0=0, condition1=0, condition2=1)
        actions = [None if self.overflow_prevention else increment_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=2, condition0=1,condition2=0)
        actions = [bebaparser.OFPExpActionSetState(state=1, table_id=self.table_id),
                   save_ts,
                   save_counter,
                   update_global_endtime__action,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)

        match = ofparser.OFPMatch(state=2, condition0=1, condition2=1)
        actions = [save_ts,
                   save_counter,
                   update_global_endtime__action,
                   update_local_endtime__action,
                   init_counter_action]
        add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)