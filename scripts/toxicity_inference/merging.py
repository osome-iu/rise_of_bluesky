from time import time
import random
import glob

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd

path = '/N/project/INCAS/bluesky/toxicity'

print('Define file path list...')
fp_list = glob.glob(f'{path}/*')

print('Shuffle file path list...')
random.shuffle(fp_list)

schema = pa.schema([
                ("uri", pa.string()),
                ("creationDate", pa.timestamp('us')),
                ("category_scores", pa.struct([
                    ("harassment", pa.float64()),
                    ("harassment_threatening", pa.float64()),
                    ("hate", pa.float64()),
                    ("hate_threatening", pa.float64()),
                    ('illicit', pa.float64()),
                    ('illicit_violent', pa.float64()),
                    ('self_harm', pa.float64()),
                    ('self_harm_instructions', pa.float64()),
                    ('self_harm_intent', pa.float64()),
                    ('sexual', pa.float64()),
                    ('sexual_minors', pa.float64()),
                    ('violence', pa.float64()),
                    ('violence_graphic', pa.float64()),
                    ])
                )
            ])

print('Create a dataset...')
dataset = ds.dataset(fp_list, format="parquet", schema=schema)

print('To table...')
df = dataset.to_table(columns=["uri", "creationDate", "category_scores"])

print('To pandas...')
df = df.to_pandas()

# sort by date
df = df.sort_values(['creationDate'])

# get the date (day-month-year) info out
df['Date'] = df['creationDate'].apply(lambda x: x.date())

# some scores are none
df['without_none'] = False
df.loc[df['category_scores'].apply(lambda x: sum([v==None for v in x.values()])==0), 'without_none'] = True

# get max score among all
df['max_score'] = None
df.loc[df['without_none'], 'max_score'] = df.loc[df['without_none'], 'category_scores'].apply(lambda x: max(x.values()))

# only without none values
df = df[df['without_none']]

print('Saving...')
df.to_parquet('/N/project/INCAS/bluesky/toxicity_merged/toxicity_merged.parquet')