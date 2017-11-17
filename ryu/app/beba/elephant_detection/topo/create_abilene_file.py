capacity_dict = {
    ('ATLA', 'HSTN'): 5e6,
    ('ATLA', 'IPLS'): 10e6,
    ('ATLA', 'WASH'): 10e6,
    ('CHIN', 'IPLS'): 10e6,
    ('CHIN', 'NYCM'): 10e6,
    ('DNVR', 'KSCY'): 10e6,
    ('DNVR', 'SNVA'): 10e6,
    ('DNVR', 'STTL'): 10e6,
    ('HSTN', 'KSCY'): 10e6,
    ('HSTN', 'LOSA'): 10e6,
    ('IPLS', 'KSCY'): 10e6,
    ('LOSA', 'SNVA'): 10e6,
    ('NYCM', 'WASH'): 10e6,
    ('SNVA', 'STTL'): 10e6,
}

nodes = set([i for i, j in capacity_dict] + [j for i, j in capacity_dict])

abilene_to_mn_mapping = {node: str(idx + 1) for idx, node in enumerate(sorted(nodes))}

access_link_capacity = 10e6  # in bps

# In bps
demands = {
    ('ATLA', 'HSTN'): ('TCP', 10e6)
}


with open('abilene.txt', 'w') as f:
    for l in capacity_dict:
        s = 's%s s%s %d' % (abilene_to_mn_mapping[l[0]], abilene_to_mn_mapping[l[1]],  capacity_dict[l]/1e6)
        print s
        f.write(s+'\n')

    for n in nodes:
        s = 'h%s s%s %d' % (abilene_to_mn_mapping[n], abilene_to_mn_mapping[n], access_link_capacity/1e6)
        print s
        f.write(s+'\n')

    print
    f.write('\n')

    for dem in demands:
        s = 'h%s h%s %s %s 0' % (abilene_to_mn_mapping[dem[0]],
                                abilene_to_mn_mapping[dem[1]],
                                 demands[dem][0],
                                '%dM' % (int(demands[dem][1]/1.0e6)) if demands[dem][1] > 0 else '0')
        print s
        f.write(s + '\n')
