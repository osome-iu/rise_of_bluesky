import glob
import os
import pandas as pd
import numpy as np
import pickle
import datetime
from tqdm import tqdm
from time import time
from graph_tool.all import *
import gzip
import struct

def read_edges_struct(file_path):
    edges = []
    with gzip.open(file_path, 'rb') as f:
        while chunk := f.read(8):  # Each edge is 8 bytes (4 bytes per int)
            edges.append(struct.unpack('ii', chunk))
    return edges

def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        running_list_nodes = pickle.load(f)
    return running_list_nodes

def network_related_analysis(base, fp, string_date, id_to_node):
    
    # read the edges
    print(f'read the edges {datetime.datetime.now()}')
    running_list_edges = read_edges_struct(fp)

    print(f'transform edges into numpy array {datetime.datetime.now()}')
    # transform edges into numpy array (see: https://graph-tool.skewed.de/static/doc/autosummary/graph_tool.Graph.html#graph_tool.Graph.add_edge_list:~:text=Note-,If,-edge_list%20is%20a)
    running_list_edges = np.array(running_list_edges)

    print(f'Create the graph {datetime.datetime.now()}')
    # Create the graph
    g = Graph()

    print(f'Create a vertex property to store string names {datetime.datetime.now()}')
    # Create a vertex property to store string names
    vname = g.new_vertex_property("string")

    print(f'Add edge list {datetime.datetime.now()}')
    # add edge list
    g.add_edge_list(running_list_edges)

    del running_list_edges

    print(f'node names to vname {datetime.datetime.now()}')
    for k,v in id_to_node.items():
        vname[k] = v

    #clustering_coef = local_clustering(g, undirected=True)

    print(f'in/out degree to dict {datetime.datetime.now()}')
    metrics = {
            "indegree": {vname[v]:v.in_degree() for v in g.vertices()},
            "outdegree": {vname[v]:v.out_degree() for v in g.vertices()},
            #"transitivity": {vname[v]:clustering_coef[v] for v in g.vertices()},
            }

    print(f'dump the distributions {datetime.datetime.now()}')
    for metric, data in metrics.items():
        with open(f'{base}/{metric}_distributions/{metric}_{string_date}.pickle', 'wb') as f:
            pickle.dump(data, f)

    # Summary statistics
    #gcc, std = global_clustering(g)
    print(f'undirected lcc {datetime.datetime.now()}')
    comp_undir, hist_undir = label_components(g, directed=False)
    print(f'directed lcc {datetime.datetime.now()}')
    comp_dir, hist_dir = label_components(g, directed=True)
    summary = {
            "day":string_date, 
            "node_count":g.num_vertices(), 
            "edge_count":g.num_edges(),
            "avg_degree":np.mean([*metrics['indegree'].values()]),
            "cc_sizes_list_undir":hist_undir,
            "largest_cc_size_undir":max(hist_undir),
            "cc_sizes_list_dir":hist_dir,
            "largest_cc_size_dir":max(hist_dir),
            #"clustering_coef":gcc,
            #"clustering_coef_std":std,
            }

    # flush metrics to free memory
    del metrics

    print(f'dump summary stats {datetime.datetime.now()}')
    with open(f'{base}/summary_stats/summary_{string_date}.pickle', 'wb') as f:
        pickle.dump(summary, f)

base = '/N/project/INCAS/bluesky/network/'

# get the id-to-node dictionary
id_to_node = load_pickle(glob.glob(f'{base}/daily_running_list_of_edges_binned/*.pickle')[0])
id_to_node = {v:k for k,v in id_to_node.items()}

# get the list of edges
folder_list = [*glob.glob(f'{base}/daily_running_list_of_edges_binned/*.bin')]

# extract the dates out
folder_list_w_dates = [(f,datetime.datetime.strptime(f.split('/')[-1].split('.')[0], "%Y-%m-%d").date()) for f in folder_list]

# sort based on dates
folder_list_w_dates = sorted(folder_list_w_dates, key=lambda x: x[1])

for fp, date_ in folder_list_w_dates:
    
    string_date = date_.strftime("%Y-%m-%d")
    print("Start processing date:", string_date)
    
    if os.path.exists(f'{base}/summary_stats/summary_{string_date}.pickle'):
        continue

    network_related_analysis(base=base, fp=fp, string_date=string_date, id_to_node=id_to_node)