import pandas as pd
import pickle

profile_creations = pd.read_parquet('/N/project/exodusbsky/processed/account_activity_count_by_day/account_activity_count_by_day_0_8.parquet', engine='pyarrow')

# take only profile creations
profile_creations = profile_creations[(profile_creations['action'] == 'create') & (profile_creations['type'] == 'app.bsky.actor.profile')]

# read user groups and map the names
with open('/N/project/INCAS/bluesky/profile_creations/user_groups.pickle', 'rb') as f:
    user_groups = pickle.load(f)

mapping = {'public_date_group_interval':'Public Access',
           'brazil_ban_interval':'Brazil X Ban',
           'block_policy_change_interval':'X Block Policy Change',
           'election_interval':'US Elections'}

user_to_group = {}
for group_name, i in user_groups.items():
    for u in i:
        user_to_group[u] = mapping[group_name]

# get user group information to the dataframe
profile_creations['user_group'] = profile_creations['author'].apply(lambda x: user_to_group.get(x))

# fill nan values with "Other"
profile_creations['user_group'] = profile_creations['user_group'].fillna('Other')

profile_creations_pivot = profile_creations.pivot_table(index='date', columns='user_group', values='count', aggfunc=sum)

profile_creations_pivot['All'] = profile_creations_pivot.sum(axis=1)

# sort by date
profile_creations_pivot = profile_creations_pivot.sort_index()

# save as parquet
profile_creations_pivot.to_parquet('/N/project/INCAS/bluesky/profile_creations/profile_creations.parquet')