import os

# It creates M CBR flows and N elastic flows with a dummy topology with just 2 switches and 1 link

M_RANGE = range(0, 20+1)
N_RANGE = range(1, 20+1)
IPERF_DURATION = 120  # s
ACCESS_LINK_CAPACITY = 50  # Mbps
CORE_LINK_CAPACITY = 50  # Mbps
CBR = '2M'

for M in M_RANGE:
    for N in N_RANGE:
        os.system('sudo python mininet_conf.py topo/2tcp.txt %d %d %d %d %d %s' %
                  (M, N, IPERF_DURATION, CORE_LINK_CAPACITY, ACCESS_LINK_CAPACITY, CBR))
