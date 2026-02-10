import os
import pandas as pd

# Load master dataset
print("Loading game metadata")
games = pd.read_excel("gameMetaData.xlsx")
print("Metadata loaded")

# Folder with review files
reviewFolder = "cleaned_csvs"

# Build Mapping
mapping = []

# get list of CSV files
files = [f for f in os.listdir(reviewFolder) if f.endswith('.csv')]
total_files = len(files)

for i, fname in enumerate(files, 1):
    appid = fname.split("_")[0] # gathers the appid from the filename

    numReviews = fname.split("_")[1].replace(".csv", "") # grabs the number of reviews from the filename

    # look up game info
    gameInfo = games[games['appid'] == int(appid)]
    if not gameInfo.empty:
        title = gameInfo.iloc[0]['title']
        mapping.append({
            "appid" : appid,
            "title" : title,
            "numReviews" : numReviews,
            "file": fname,
        })
    # Progress tracking
    print(f"[{i}/{total_files}] Processed file: {fname}") 

# Convert to DataFrame for easy viewing
mapping_df = pd.DataFrame(mapping)
print("\nFinal Mapping")
print(mapping_df)

            