import glob
import pandas as pd
import pickle
from tqdm import tqdm
import datetime
from time import time
import itertools
import gzip
import struct


def write_edges_struct(file_path, running_list_edges):
    with gzip.open(file_path, 'wb') as f:
        for edge in running_list_edges:
            f.write(struct.pack('ii', *edge))

def read_edges_struct(file_path):
    edges = []
    with gzip.open(file_path, 'rb') as f:
        while chunk := f.read(8):  # Each edge is 8 bytes (4 bytes per int)
            edges.append(struct.unpack('ii', chunk))
    return set(edges)

def save_pickle(file_path, running_list_nodes):
    with open(file_path, 'wb') as f:
        pickle.dump(running_list_nodes, f)

def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        running_list_nodes = pickle.load(f)
    return running_list_nodes

def reindex(entry):
    idx = running_dict_nodes.get(entry)
    if not idx:
        idx = len(running_dict_nodes)
        running_dict_nodes[entry] = idx
    return idx


# define the dates that will be saved
with open('/N/u/oseckin/BigRed200/bluesky_brazilian_migration/scripts/daily_running_list_of_edges/dates_to_save.pickle', 'rb') as f:
    dates_to_save = pickle.load(f)

# log start time
start = time()

# get the list of folders for parquet files
folder_list = glob.glob('/N/project/exodusbsky/processed/network/follows_unfollows_wout_bsky_0_8.parquet/date*')

# get the date info out
folder_list_w_dates = [(f,datetime.datetime.strptime(f.split('=')[-1], "%Y-%m-%d").date()) for f in folder_list]

# sort by date
folder_list_w_dates = sorted(folder_list_w_dates, key=lambda x: x[1])

# create a dictionary where key = date, value = list of parquet files
date_to_file_dict = {d:glob.glob(f"{fp}/*") for fp,d in folder_list_w_dates}

# define the base folder to save the running list of edges and node to id dict
save_base = '/N/project/INCAS/bluesky/network/daily_running_list_of_edges_binned/'

if len(glob.glob(f"{save_base}/*")) == 0:
    # since there is no file, we will start from scratch
    files_to_go_over = [*date_to_file_dict.items()]
    running_list_edges = set()
    running_dict_nodes = dict()

else:
    dates = [f.split('/')[-1].split('.')[0] for f in glob.glob(f"{save_base}/*.bin")]
    max_date_datetime, max_date_str = max([(datetime.datetime.strptime(d, "%Y-%m-%d").date(), d) for d in dates])
    files_to_go_over = [*{k:v for k,v in date_to_file_dict.items() if k>max_date_datetime}.items()]
    running_list_edges = read_edges_struct(f"{save_base}/{max_date_str}.bin")
    running_dict_nodes = load_pickle(f"{save_base}/node_to_id.pickle")

print("We have data until:", max_date_str)

for date, file_list in files_to_go_over:
    string_date = date.strftime("%Y-%m-%d")
    print("Start processing date:", string_date)

    for fp in tqdm(file_list):
        
        # read the parquet file
        temp = pd.read_parquet(fp, engine='pyarrow')[['author', 'subjectAuthor', 'action']]
        
        # get creations
        creations = temp[temp['action'] == 'create'][['author', 'subjectAuthor']]
        
        # transform into a set (with users transformed into index numbers)
        creations = {(reindex(a), reindex(s)) for a,s in zip(creations['author'],creations['subjectAuthor'])}

        # get deletions
        deletions = temp[temp['action'] != 'create'][['author', 'subjectAuthor']]
        deletions = {(reindex(a), reindex(s)) for a,s in zip(deletions['author'],deletions['subjectAuthor'])}
        
        # update the running list of edges
        # add the creations
        running_list_edges.update(creations)
        
        # remove the deletions
        running_list_edges -= deletions

        # flush memory
        del temp, creations, deletions
    
    if date in dates_to_save:
        write_edges_struct(f"{save_base}/{string_date}.bin", running_list_edges)
        save_pickle(f"{save_base}/node_to_id.pickle", running_dict_nodes)