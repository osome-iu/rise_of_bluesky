import glob
import pandas as pd
from tqdm import tqdm

# create the file path list and sort
cc_fp_list = glob.glob('/N/project/exodusbsky/processed/network/FollowerNetworkPropertiesNew/*.pkl')
cc_fp_list = sorted([(fp, datetime.datetime.strptime(fp.split('_')[-1].split('.')[0], '%Y-%m-%d').date()) for fp in cc_fp_list], key=lambda x: x[1])

avg_degree = {}
clustering_coef = {}
for fp, date in tqdm(cc_fp_list):
    temp = pd.read_pickle(fp)
    cc = temp['clustering_coefficients']
    clustering_coef[date] = np.nanmean(cc)

clustering_coef = pd.DataFrame(clustering_coef.items())

# set index as the date
clustering_coef.index = clustering_coef[0]

# drop col
clustering_coef = clustering_coef.drop(columns=[0])

# rename the col
clustering_coef.columns = ['Clustering Coefficient']