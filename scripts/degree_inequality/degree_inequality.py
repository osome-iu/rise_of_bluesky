import numpy as np
import pandas as pd
import glob
import os
import datetime
import matplotlib.pyplot as plt
import pickle
import json
from tqdm import tqdm

def gini(array):
    """Calculate the Gini coefficient of a numpy array."""
    # based on bottom eq: http://www.statsdirect.com/help/content/image/stat0206_wmf.gif
    # from: http://www.statsdirect.com/help/default.htm#nonparametric_methods/gini.htm
    if np.amin(array) < 0:
        array -= np.amin(array) #values cannot be negative
    array += 0.0000001 #values cannot be 0
    array = np.sort(array) #values must be sorted
    index = np.arange(1,array.shape[0]+1) #index per array element
    n = array.shape[0]#number of array elements
    
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array))) #Gini coefficient

def kappa(array):
    
    sum_of_squares = sum(array**2)/len(array)
    square_of_avg = np.average(array)**2

    return sum_of_squares/square_of_avg

fp_list = sorted(glob.glob('/N/project/INCAS/bluesky/network/indegree_distributions/*'))
fp_list_w_dates = [(f, datetime.datetime.strptime(f.split('_')[-1].split('.')[0], "%Y-%m-%d").date()) for f in fp_list]
fp_list_w_dates = sorted(fp_list_w_dates, key=lambda x: x[1])

for fp, date in tqdm(fp_list_w_dates):

    output_fp = f"/N/project/INCAS/bluesky/network/gini_and_kappa/{str(date)}.json"

    if os.path.exists(output_fp):
        continue
    
    with open(fp, 'rb') as _:
        data = pickle.load(_)
    
    # transform into a np array
    data = np.array(list(data.values()), dtype=float)
    g = gini(data)
    k = kappa(data)
    with open(output_fp, 'w') as _:
        json.dump({'gini':g, 'kappa':k}, _)