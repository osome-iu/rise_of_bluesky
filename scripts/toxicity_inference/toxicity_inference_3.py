import json
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

from time import time, sleep
import glob

import os
import re

from datetime import date, timedelta
from collections import defaultdict, Counter
import pickle

from pathlib import Path
from tqdm.auto import tqdm
import pyarrow as pa
import pyarrow.parquet as pq

from openai import OpenAI

from multiprocessing import Pool

# set up the OpenAI client
with open('./moderation_config.json', 'r') as f:
    config = json.load(f)

client = OpenAI(api_key=config['3'])

def get_toxicity(df, client):
    
    # reset index
    df = df.reset_index(drop=True)

    # create the list of batches
    N = len(df)

    batch_list = [*range(0,N+200,200)]
    batch_list = [(s,e) for s,e in zip(batch_list[:-1],batch_list[1:])]

    for batch in batch_list:

        start, end = batch
        
        uri_start = df[start:end]['uri_end'].iloc[0]
        uri_end = df[start:end]['uri_end'].iloc[-1]
        SAVE_NAME = f'/N/project/INCAS/bluesky/toxicity/batch_{uri_start}_{uri_end}.gzip.parquet'
        
        # pass if file exists
        if os.path.exists(SAVE_NAME):
            pass
        
        else:
            print(f"Start:{start} - End:{end}")    

            try:
                response = client.moderations.create(
                                                    model = "omni-moderation-latest",
                                                    input = [*df[start:end]['text']],
                                                    )
            except:
                sleep(5)
                response = client.moderations.create(
                                                    model = "omni-moderation-latest",
                                                    input = [*df[start:end]['text']],
                                                    )   

            # wait 1 second before moving on to the next mini (200) batch
            sleep(1)

            # restructure the output into a nested json file
            results_structured = [{k:v for k,v in dict(r).items() if k!='category_applied_input_types'} for r in response.results]

            # transform everything into a dictionary
            results_structured = [{'categories':dict(r['categories']), 'category_scores':dict(r['category_scores']), 'flagged':r['flagged']} for r in results_structured]

            # prepare the dataframe to save
            save = pd.DataFrame(results_structured).merge(df[['uri','text','creationDate']][start:end].reset_index(drop=True), left_index=True, right_index=True)

            # save
            save = save[['uri', 'creationDate', 'categories', 'category_scores', 'flagged']]
            save.to_parquet(SAVE_NAME)

            # check if it's the beginning of a 2000 batch
            if end % 2000 == 200:
                print(f"end % 2000 == 200 --> {end}")
                time_1 = time()

            # check if it's the ending of a 2000 batch
            elif end % 2000 == 0:
                print(f"end % 2000 == 0 --> {end}")
                time_2 = time()

                # how many seconds have passed from the first batch of 2000 to the last one?
                try:
                    time_diff = time_2 - time_1
                except:
                    time_diff = 0

                # wait until a minute is completed
                wait = 60 - time_diff
                print(f'Waiting for {wait} seconds')

                if wait < 1:
                    wait = 60
            
                sleep(wait)


for activity_table_path in glob.glob('/N/project/exodusbsky/bluesky_activities/bluesky_posts.*.parquet'):
    
    parquet_file = pq.ParquetFile(activity_table_path)
    
    N = parquet_file.num_row_groups
    
    for i in tqdm(range(N)):
        temp = parquet_file.read_row_group(i).to_pandas()

        # drop nan values
        temp = temp.dropna(subset=['text'])

        # will be used for naming
        temp['uri_end'] = temp['uri'].apply(lambda x: x.split('/')[-1])

        # take 10% sample
        temp = temp.sample(frac=.1, random_state=89)

        # split into 2 and keep going
        dfs = np.array_split(temp, 2)
        
        try:
            get_toxicity(dfs[0], client)
        except:
            sleep(5)
            pass