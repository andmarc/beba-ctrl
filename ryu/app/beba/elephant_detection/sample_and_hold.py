import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import ryu.ofproto.beba_v1_0 as bebaproto
import ryu.ofproto.beba_v1_0_parser as bebaparser
from utils import add_flow


class SampleAndHold:
    def __init__(self, table_id, map_proto_tableid, ip_proto_values, sample_interval ):
        self.table_id = table_id
        self.map_proto_tableid = map_proto_tableid
        self.ip_proto_values = ip_proto_values
        self.sample_interval = sample_interval

    def configure_stateful_table(self, datapath):
        req = bebaparser.OFPExpMsgConfigureStatefulTable(datapath=datapath, table_id= self.table_id, stateful=1)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath,
                                             command=bebaproto.OFPSC_EXP_SET_L_EXTRACTOR,
                                             fields=[ofproto.OXM_OF_ETH_SRC],
                                             table_id= self.table_id)
        datapath.send_msg(req)

        req = bebaparser.OFPExpMsgKeyExtract(datapath=datapath,
                                             command=bebaproto.OFPSC_EXP_SET_U_EXTRACTOR,
                                             fields=[ofproto.OXM_OF_ETH_SRC],
                                             table_id= self.table_id)
        datapath.send_msg(req)

        # GD[0] is sample counter
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=self.table_id, global_data_variable_id=0,
                                                         value=self.sample_interval-1)
        datapath.send_msg(req)

        # GD[1] is sample interval
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=self.table_id, global_data_variable_id=1,
                                                         value=self.sample_interval-1)
        datapath.send_msg(req)

########################################################################################################################

        # C0: check if the sample counter (GD[0]) is equal to sample_interval (GD[1])
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=self.table_id,
                                               condition_id=0,
                                               condition=bebaproto.CONDITION_EQ,
                                               operand_1_gd_id=0,
                                               operand_2_gd_id=1)
        datapath.send_msg(req)

########################################################################################################################

        #GD[0]+=1
        increment_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                                          opcode=bebaproto.OPCODE_SUM,
                                                                          output_gd_id=0,
                                                                          operand_1_gd_id=0,
                                                                          operand_2_cost=1)

        #GD[0]=0
        reset_counter_action = bebaparser.OFPExpActionSetDataVariable(table_id=self.table_id,
                                                                      opcode=bebaproto.OPCODE_SUB,
                                                                      output_gd_id=0,
                                                                      operand_1_gd_id=0,
                                                                      operand_2_gd_id=0)

########################################################################################################################

        """if is_cond: reset_counter_action else: increment_counter_action"""
        for is_cond, action in enumerate([increment_counter_action,reset_counter_action]):
            """Add sample metadata (0/1) and send to TCP/UDP stateful table"""
            for i in self.ip_proto_values:
                    match = ofparser.OFPMatch(eth_type=0x0800, ip_proto=self.ip_proto_values[i], condition0=is_cond)
                    add_flow(datapath=datapath,
                             table_id=self.table_id,
                             priority=10,
                             match=match,
                             actions = [action],
                             inst2=[ofparser.OFPInstructionGotoTable(self.map_proto_tableid[i]),
                                    ofparser.OFPInstructionWriteMetadata(is_cond, 0x1)])

            """Match all the packet not matched above"""
            match = ofparser.OFPMatch(condition0=is_cond)
            actions = [action]
            add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match, actions=actions)