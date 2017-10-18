import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import pickle


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

def dpid_from_name(name):
    return int(name[1:])

def get_from_mininet():
    with open("data.pkl", "rb") as fh:
        mininet_data = pickle.load(fh)
    topo = mininet_data[0]
    addresses = mininet_data[1]
    return topo, addresses