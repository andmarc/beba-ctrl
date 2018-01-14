import os, glob
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm


class colorgen:
    def __init__(self, n):
        self.i = 0
        self.n = n
        self.colors = cm.rainbow(np.linspace(0, 1, n))

    def __iter__(self):
        return self

    def next(self):
        if self.i < self.n:
            i = self.i
            self.i += 1
            return self.colors[i]
        else:
            raise StopIteration()


def plot(results, M, N, CORE_LINK_CAPACITY, CBR):
    links = set(sum(map(lambda x: x.keys(), results), []))
    print links
    for link in links:
        flows_measures = [result[link] for result in results]
        print flows_measures
        flows = set(sum(map(lambda x: x.keys(), flows_measures), []))
        # sort by name length to split TCP from CBRs
        flows = sorted(sorted(flows, key = lambda x : x[0]), key = lambda x : x[1])
        print flows
        ind = 2 * np.arange(len(flows_measures))
        width = 1.0/(len(flows))
        fig, ax = plt.subplots(figsize=(8, 4))
        rects = []
        MAX = 0
        color = colorgen(len(flows))
        for idx, flow in enumerate(list(flows)):
            curr_flow_measures = [measure[flow] * 8.0 / 1e6 if flow in measure else 0 for measure in flows_measures]
            print flow, curr_flow_measures
            MAX = max(MAX, curr_flow_measures)
            rects.append(ax.bar(ind + width * (idx + 1), curr_flow_measures, width, zorder=3, color=color.next())[0])

        # add some text for labels, title and axes ticks
        ax.set_ylabel('Rate [Mbps]')
        ax.set_title('Elephant flows on link %s @%dMbps (%d CBR @%sbps, %d elastic)' % (link, CORE_LINK_CAPACITY, M, CBR, N))
        ax.set_xticks(ind + 2 * width)
        ax.set_xticklabels(range(len(flows_measures)))
        ax.set_xlim([ax.get_xlim()[0], len(flows_measures) + 1])
        ax.set_ylim([0, ax.get_ylim()[1] * 1.3])

        ax.legend(rects, flows, loc='upper right', prop={'size': 8})

        plt.grid(zorder=0)
        plt.tight_layout()
        plt.show()  # block=False)

os.system('sshpass -p mininet scp -P 4567 root@0:/home/mininet/beba-ctrl/ryu/app/beba/elephant_detection/*txt .')

for file in glob.glob('*.txt'):
    print file
    with open(file) as f:
        content = f.readlines()
        results = [eval(content[2*i + 1].strip()) for i in range(len(content)/2)]
        M, N, CORE_LINK_CAPACITY = map(int, file.split('.')[-5: -2])
        CBR = file.split('.')[-2]
        plot(results, M, N, CORE_LINK_CAPACITY, CBR)
