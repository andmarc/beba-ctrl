import ryu.ofproto.ofproto_v1_3 as ofproto
import ryu.ofproto.ofproto_v1_3_parser as ofparser
import pickle,time


def add_flow(datapath, table_id, priority, match, actions, inst2=None, hard_timeout=0, idle_timeout=0):
    actions = filter(lambda x: x is not None, actions)
    if len(actions) > 0:
        inst = [ofparser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
    else:
        inst = []
    if inst2 is not None:
        inst += inst2
    mod = ofparser.OFPFlowMod(datapath=datapath, table_id=table_id,
                              priority=priority, match=match, instructions=inst, hard_timeout=hard_timeout,
                              idle_timeout=idle_timeout)
    datapath.send_msg(mod)

def dpid_from_name(name):
    return int(name[1:])

def get_from_mininet():
    with open("data.pkl", "rb") as fh:
        mininet_data = pickle.load(fh)
    topo = mininet_data[0]
    addresses = mininet_data[1]
    return topo, addresses

def get_switch_time():
    return int(time.time()*1000) & 0xFFFFFFFF

def bps_to_human_string(num, Bps=False):
    if Bps:
        # convert bit per sec to byte per sec
        num = num/8.0
        suffix = 'B/s'
    else:
        suffix = 'bps'

    for unit in ['', 'K', 'M', 'G']:
        if abs(num) < 1000.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f %s%s" % (num, 'T', suffix)

def red_msg(text):
    print '\x1B[31m' + text + '\x1B[0m'

def green_msg(text):
    print '\x1B[32m' + text + '\x1B[0m'
