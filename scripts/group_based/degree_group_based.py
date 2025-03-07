import pickle
import numpy as np
import os
import pandas as pd
import glob
import datetime

with open('/N/project/INCAS/bluesky/profile_creations/user_groups.pickle', 'rb') as f:
    user_groups = pickle.load(f)

with open('/N/project/INCAS/bluesky/profile_creations/user_group_date_intervals.pickle', 'rb') as f:
    user_group_date_intervals = pickle.load(f)

fp_list_indegree = glob.glob('/N/project/INCAS/bluesky/network/indegree_distributions/*')
fp_list_indegree = sorted(fp_list_indegree)
fp_list_w_dates_indegree = [(f, datetime.datetime.strptime(f.split('/')[-1].split('_')[-1].split('.')[0], "%Y-%m-%d").date()) for f in fp_list_indegree]

fp_list_outdegree = glob.glob('/N/project/INCAS/bluesky/network/outdegree_distributions/*')
fp_list_outdegree = sorted(fp_list_outdegree)
fp_list_w_dates_outdegree = [(f, datetime.datetime.strptime(f.split('/')[-1].split('_')[-1].split('.')[0], "%Y-%m-%d").date()) for f in fp_list_outdegree]


for indegree_fp, outdegree_fp, date in zip(fp_list_indegree, 
                                fp_list_outdegree, 
                                [datetime.datetime.strptime(f.split('/')[-1].split('_')[-1].split('.')[0], "%Y-%m-%d").date() for f in fp_list_outdegree]):

    str_date = str(date)
    print(f"\n------------------------------{str_date} being processed!")

    save_path = f'/N/project/INCAS/bluesky/network/group_degree_distributions/group_based_degrees_{str_date}.pickle'
    
    if os.path.exists(save_path):
        continue

    with open(indegree_fp, 'rb') as f:
        indegree = pickle.load(f)
    
    with open(outdegree_fp, 'rb') as f:
        outdegree = pickle.load(f)

    log = {}
    for group_name, users in user_groups.items():
        print(group_name)
        log[group_name] = {'avg_indegree':0, 'avg_outdegree':0, 'indegree_dist':None, 'outdegree_dist':None}
        # start once the group is emerged
        if date >= user_group_date_intervals[group_name]['start']:
            group_indegree_dist = [indegree.get(u,0) for u in users]
            avg_group_indegree = np.mean(group_indegree_dist)
            log[group_name]['avg_indegree'] = avg_group_indegree
            log[group_name]['indegree_dist'] = group_indegree_dist

            group_outdegree_dist = [outdegree.get(u,0) for u in users]
            avg_group_outdegree = np.mean(group_outdegree_dist)
            log[group_name]['avg_outdegree'] = avg_group_outdegree
            log[group_name]['outdegree_dist'] = group_outdegree_dist
    
    print(f"{str_date} dumping in a pickle file...")
    with open(save_path, 'wb') as f:
        pickle.dump(log, f)