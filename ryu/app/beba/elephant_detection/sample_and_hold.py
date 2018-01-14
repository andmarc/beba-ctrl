import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import ryu.ofproto.beba_v1_0 as bebaproto
import ryu.ofproto.beba_v1_0_parser as bebaparser
from utils import add_flow


class SampleAndHold:
    def __init__(self, table_id, map_proto_tableid, ip_proto_values, sample_probability ):
        self.table_id = table_id
        self.map_proto_tableid = map_proto_tableid
        self.ip_proto_values = ip_proto_values
        self.random_threshold = int(sample_probability*(2**16 - 1))

    def configure_stateful_table(self, datapath):
        req = bebaparser.OFPExpMsgConfigureStatefulTable(datapath=datapath, table_id=self.table_id, stateful=1)
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

        # GD[0] is random threshold
        req = bebaparser.OFPExpMsgsSetGlobalDataVariable(datapath=datapath, table_id=self.table_id, global_data_variable_id=0,
                                                         value=self.random_threshold)
        datapath.send_msg(req)

########################################################################################################################

        # HF_0 holds a unsigned int 16 bit random value
        req = bebaparser.OFPExpMsgHeaderFieldExtract(
                datapath=datapath,
                table_id=self.table_id,
                extractor_id=0,
                field=bebaproto.OXM_EXP_RANDOM
            )
        datapath.send_msg(req)

########################################################################################################################

        # C0: check if the 16 bit random uint is lower or equal the threshold
        req = bebaparser.OFPExpMsgSetCondition(datapath=datapath,
                                               table_id=self.table_id,
                                               condition_id=0,
                                               condition=bebaproto.CONDITION_LTE,
                                               operand_1_hf_id=0,
                                               operand_2_gd_id=0)
        datapath.send_msg(req)

########################################################################################################################

        """if is_cond: sample else not sample"""
        for is_cond in range(2):
            """Add sample metadata (0/1) and send to TCP/UDP stateful table"""
            for i in self.ip_proto_values:
                    match = ofparser.OFPMatch(eth_type=0x0800, ip_proto=self.ip_proto_values[i], condition0=is_cond)
                    add_flow(datapath=datapath,
                             table_id=self.table_id,
                             priority=1,
                             match=match,
                             inst2=[ofparser.OFPInstructionGotoTable(self.map_proto_tableid[i]),
                                    ofparser.OFPInstructionWriteMetadata(is_cond, 0x1)])

            """Match all the packet not matched above"""
            match = ofparser.OFPMatch(condition0=is_cond)
            add_flow(datapath=datapath, table_id=self.table_id, priority=0, match=match)