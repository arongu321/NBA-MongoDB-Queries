import json
from pymongo import MongoClient
import sys

client = MongoClient('mongodb://localhost:{}'.format(sys.argv[1]))

# Open database in MongoDB server
db = client["nba_db"]

# Open player_stats collection in databse
statsCollection = db["player_stats"]

# Delete any existing documents in collection
statsCollection.delete_many({})

# Load JSON file data to be inserted in collection
with open("nba_data.json", "r") as file:
    data = json.load(file)

# Get array of NBA player stats from 2018 to 2024
playerStats = data["player_stats"]

# Insert player stats documents into collection
statsCollection.insert_many(playerStats)

