import os

# It creates M CBR flows and N elastic flows with a dummy topology with just 2 switches and 1 link

P_SAMPLE_RANGE = [1, 0.1, 0.01, 0.001]
M_RANGE = [0,5,10] #range(0, 20+1)
N_RANGE = [1,5,10,15,20,25] #range(1, 20+1)
IPERF_DURATION = 120  # s
ACCESS_LINK_CAPACITY = 50  # Mbps
CORE_LINK_CAPACITY = 50  # Mbps
CBR = '4M'

for P_SAMPLE in P_SAMPLE_RANGE:
    for M in M_RANGE:
        for N in N_RANGE:
            os.system('sudo python mininet_conf.py topo/2tcp.txt %d %d %d %d %d %.3f %s' %
                      (M, N, IPERF_DURATION, CORE_LINK_CAPACITY, ACCESS_LINK_CAPACITY, P_SAMPLE, CBR))
