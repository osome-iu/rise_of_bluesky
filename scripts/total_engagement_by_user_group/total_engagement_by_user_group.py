import pandas as pd
import datetime
import pickle

with open('/N/project/INCAS/bluesky/profile_creations/user_groups.pickle', 'rb') as f:
    user_groups = pickle.load(f)

group_name_map = {'public_date_group': 'Public Registration', 'brazil_ban': 'Brazilian Migration', 'block_policy_change':'Twitter/X Block Policy Change', 'election':'Elections'}
user_groups = {group_name_map[k.replace('_interval','')]:v for k, v in user_groups.items()}

user_to_group = {}

for group_name, v in user_groups.items():
    for i in v:
        user_to_group[i] = group_name

activity = pd.read_parquet('/N/project/exodusbsky/processed/account_activity_count_by_day/account_activity_count_by_day_0_8.parquet', engine='pyarrow')

# bound by 2023 August 18
activity = activity[activity['date']>=datetime.datetime(2023,8,19).date()]

# take only the creations
activity = activity[activity['action'] == 'create']

# put user group info
activity['user_group'] = activity['author'].apply(lambda x: user_to_group.get(x))

# total engagement
FILTER = activity['type'].isin({'app.bsky.feed.like', 'app.bsky.feed.post', 'app.bsky.feed.repost'})
total_engagement = activity[FILTER].pivot_table(index='date', values='count', aggfunc='sum')
total_engagement.columns = ['All']

# engagement by user group
engagement_by_user_group = activity[FILTER].pivot_table(index='date', columns='user_group', values='count', aggfunc='sum')

# merge
total_engagement = total_engagement.merge(engagement_by_user_group, right_index=True, left_index=True)

# save
total_engagement.to_parquet('/N/project/exodusbsky/processed/account_activity_count_by_day/total_engagement_by_day_by_user.parquet')