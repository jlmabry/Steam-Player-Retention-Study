import json
import pandas as pd

# Load and Normalize the JSON

with open("C:/Users/justi/OneDrive/School/CSCI-B365/courseProject/datasetMaterial/games.json", encoding="utf-8") as f:
    gamesData = json.load(f)

# convert dictionary to a list of game entries

gamesList = []
for appid, game in gamesData.items():
    game['appid'] = appid 

    gamesList.append(game)
# Normalize into a flat table
gamesDf = pd.json_normalize(gamesList)

# Save to CSV
gamesDf.to_csv("C:/dataset/gameMetaData.csv", index = False)

