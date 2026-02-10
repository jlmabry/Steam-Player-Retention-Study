import pandas as pd

# Load metadata
metadata = pd.read_excel('gameMetaData.xlsx')
metadata.columns = [c.lower() for c in metadata.columns]

# Load recommendations
summary = pd.read_excel('recommendation_summary.xlsx')
summary.columns = [c.lower() for c in summary.columns]

# Merge datasets on 'appid'
combined = pd.merge(summary, metadata, on='appid', how='inner')
# select one of the two titles to bring to the new dataset
combined = combined.rename(columns={"title_x": "title"})

# Select relevant collumns
tag_cols = [c for c in combined.columns if c.startswith("tags.")]
final_df = combined[['appid','title', 'verdict', 'average_playtime_forever', 'genres'] + tag_cols]

# Save dataset
final_df.to_excel('game_dataset_combined.xlsx', index=False)

print("Unified dataset created with", len(final_df), "games.")
print(final_df.head())