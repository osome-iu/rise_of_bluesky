import pickle
import pandas as pd

with open('/N/project/INCAS/bluesky/profile_creations/user_groups.pickle', 'rb') as f:
    user_groups = pickle.load(f)

account_activity_count_by_day = pd.read_parquet('/N/project/exodusbsky/processed/account_activity_count_by_day/account_activity_count_by_day_0_8.parquet')

types = {'app.bsky.feed.like', 'app.bsky.feed.post', 'app.bsky.feed.repost'}

group_based_summary = {}

for group_name, s in user_groups.items():
    print(group_name)
    FILTER = (account_activity_count_by_day['author'].isin(s)) & (account_activity_count_by_day['type'].isin(types))
    temp = account_activity_count_by_day[FILTER].pivot_table(index='date', values='count', aggfunc=['sum','count'])
    temp.columns = ['Total Engagement', 'User Count']
    temp['Group Name'] = group_name
    group_based_summary[group_name] = temp

for group_name, d in group_based_summary.items():
    d.to_parquet(f"/N/project/exodusbsky/processed/account_activity_count_by_day/{group_name}.parquet")