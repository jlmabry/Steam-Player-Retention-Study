import pandas as pd

# Load metadata
metadata = pd.read_excel('gameMetaData.xlsx')
metadata.columns = [c.lower() for c in metadata.columns]

# Load recommendation summary
summary = pd.read_excel('recommendation_summary.xlsx')
summary.columns = [c.lower() for c in summary.columns]

# Merge on appid
merged = pd.merge(summary, metadata, on='appid', how='inner')

# Identify tag columns
tag_cols = [c for c in merged.columns if c.startswith('tags.')]

# Split the data into recommended and not recommended
recommended_games = merged[merged["verdict"] == "Recommended"]
not_recommended_games = merged[merged["verdict"] == "Not Recommended"]

# Aggregate tag votes
recommended_tags = recommended_games[tag_cols].sum().sort_values(ascending = False)
not_recommended_tags = not_recommended_games[tag_cols].sum().sort_values(ascending = False)

# Convert to dataframes for readability
recommended_summary = recommended_tags.reset_index()
recommended_summary.columns = ['tag', 'total_votes']

not_recommended_summary = not_recommended_tags.reset_index()
not_recommended_summary.columns = ['tag', 'total_votes']

# Save results
with pd.ExcelWriter('tag_popularity_by_verdict.xlsx', engine="openpyxl") as writer:
    recommended_summary.to_excel(writer, sheet_name='Recommended', index=False)
    not_recommended_summary.to_excel(writer, sheet_name='Not Recommended', index=False)

print("Top tags for Recommended Games:")
print(recommended_summary.head(10))
print("Top tags for not recommended games")
print(not_recommended_summary.head(10))