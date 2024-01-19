"""
Answers mongoDB queries from nba_db collection
"""
from pymongo import MongoClient
import json
import sys

client = MongoClient('mongodb://localhost:{}'.format(sys.argv[1]))

# Open database in MongoDB server
db = client["nba_db"]

# Open player_stats collection in databse
statsCollection = db["player_stats"]

# Q1: Retrieve all the player names with James that played in the NBA from 2022 and onwards with name sorted in ascending order
print("----------------------------------")
print("Q1")
print("----------------------------------")
results = statsCollection.aggregate([
    {
        "$match": {
            "year": {"$gte": 2022},
            "name": {"$regex": "James", "$options": "i"}
        }
    },
    {
        "$group": {
            "_id": {"name": "$name"}
        }
    },
    {
        "$project": {
            "name": "$_id.name",
            "_id": 0
        }
    },
    {
        "$sort": {
            "name": 1
        }
    }
])

for result in results:
    print(result)


# Q2: Retrieve the name, team, and year, and PTS of players with the top 3 PTS in the regular season from 2019 and onwards
# Note: Not necessary to include all ties
# Note: regular season implies season_type field is Regular Season
print("----------------------------------")
print("Q2")
print("----------------------------------")
results = statsCollection.aggregate([
    {
        "$match": {
            "season_type": "Regular Season",
            "year":{"$gte": 2019}
        }
    },
    {
        "$sort": {
            "PTS": -1
        }
    },
    {
        "$limit": 3
    },
    {
        "$project": {
            "_id": 0,
            "name": 1,
            "team": 1,
            "year": 1,
            "PTS": 1
        }
    }
])
for result in results:
    print(result)

# Q3: Retrieve the team and number of players who played in the post season for each time from 2020 and onwards from the most to the least number of players
# Note: This implies that season_type field is Post Season
print("----------------------------------")
print("Q3")
print("----------------------------------")
results = statsCollection.aggregate([
    {
        '$match': {
            "season_type": "Post Season",
            "year": {"$gte": 2020}
        }
    },
    {
        '$group': {
            '_id': '$team',
            'unique_players': {'$addToSet': '$name'}
        }
    },
    {
        '$project': {
            'team': '$_id',
            'number_players': {'$size': '$unique_players'},
            '_id': 0 
        }
    },
    {
        "$sort": {
            'number_players': -1
        }
    }
])

for result in results:
    print(result)

# Q4: Retrieve the player names and their total number of points(round to nearest integer) for all the players that played on the Los Angeles Lakers in the 2021 regular season from most points to least points
# Note: GP is games played and PTS is points per game
print("----------------------------------")
print("Q4")
print("----------------------------------")
results = statsCollection.aggregate([
    {
        '$match': {
            'team': 'Los Angeles Lakers', 
            'year': 2021,           
            'season_type': 'Regular Season'
        }
    },
    {
        '$addFields': {
            'total_points': {
                '$toInt': {
                    '$ceil': {
                        '$multiply': ['$PTS', '$GP']
                    }
                }
            }
        }
    },
    {
        '$group': {
            '_id': '$name',  
            'total_points': {'$sum': '$total_points'}
        }
    },
    {
        '$project': {
            'name': '$_id',                    
            'total_points': 1,         
            '_id': 0                          
        }
    },
    {
        '$sort': {
            'total_points': -1
        }
    }
])

for result in results:
    print(result)

# Q5: Retrieve the player names, year, and teams for players that played for more than two teams in the same year from 2021 and onwards with name and year sorted in ascending order
print("----------------------------------")
print("Q5")
print("----------------------------------")
results = statsCollection.aggregate([
    {
        '$match': {
            'year': {"$gte": 2021}
        }
    },
    {
        '$group': {
            '_id': {
                'name': '$name',
                'year': '$year'
            },
            'unique_teams': {'$addToSet': '$team'},
        }
    },
    {
        "$addFields": {
            'count': {'$size': '$unique_teams'}
        }
    },
    {
        '$match': {
            'count': {'$gt': 2}
        }
    },
    {
        '$project': {
            '_id': 0,
            'name': '$_id.name',
            'year': '$_id.year',
            'teams': '$unique_teams'
        }
    },
    {
        '$sort': {'name': 1, 'year': 1}
    }
])

for result in results:
    print(result)

# Q6: Retrieve player name and position of players that played in the 2021 and 2023 post seasons but not in the 2022 post season with name sorted in descending order
print("----------------------------------")
print("Q6")
print("----------------------------------")

results = statsCollection.aggregate([
    {
        '$match': {
            '$or': [
                {'year': 2021, 'season_type': 'Post Season'},
                {'year': 2022, 'season_type': 'Post Season'},
                {'year': 2023, 'season_type': 'Post Season'},
            ],
            'position': 'C'
        }
    },
    {
        '$group': {
            '_id': '$name',
            'years': {'$addToSet': '$year'},
            'position': {'$first': '$position'},
        }
    },
    {
        '$match': {
            'years': {'$in': [2021, 2023], '$nin': [2022]}
        }
    },
    {
        '$project': {
            '_id': 0,
            'name': '$_id'
        }
    },
    {
        '$sort': {
            'name': -1
        }
    }
])

for result in results:
    print(result)

# Q7: Retrieve the player name, points per game and number of turnovers per game over the 2019-2022 regular season of players that average more than 3 turnovers a game over those seasons sorting the turnovers per game in descending order
# Note: Do not take the average of the turnovers(TOV) over those seasons as the players may have not played the same number of games played
# Note: Only include players who played in all 4 regular seasons
print("----------------------------------")
print("Q7")
print("----------------------------------")

results = statsCollection.aggregate([
    {
        '$match': {
            'year': {'$gte': 2019, '$lte': 2022},
            'season_type': 'Regular Season'
        }
    },
    {
        '$group': {
            '_id': '$name',
            'totalTOV': {'$sum': {'$multiply': ['$TOV', '$GP']}},
            'totalPTS': {'$sum': {'$multiply': ['$PTS', '$GP']}},
            'totalGP': {'$sum': '$GP'},
            'years': {'$addToSet': '$year'}
        }
    },
    {
        '$project': {
            '_id': 0,
            'name': '$_id',
            'PPG': {'$divide': ['$totalPTS', '$totalGP']},
            'TPG': {'$divide': ['$totalTOV', '$totalGP']},
            'yearsCount': {'$size': '$years'}
        }
    },
    {
        '$match': {
            'TPG': {'$gt': 3},
            'yearsCount': 4
        }
    },
    {
        '$sort': {
            'TPG': -1
        }
    },
    {
        '$project': {
            '_id': 0,
            'name': '$name',
            'PPG': '$PPG',
            'TPG': '$TPG'
        }
    }
])

for result in results:
    print(result)

# Q8: Retrieve the average field goal percentage for each regular season and postseason from the highest to lowest average field goal percentage
# Note: Field goal percentage is calculated by FGM/FGA, DO NOT USE the average of FG% as this does not account for the number of games each player plays
# Note: FGM is field goals made per game and FGA is field goals attempted per game
print("----------------------------------")
print("Q8")
print("----------------------------------")

results = statsCollection.aggregate([
    {
        '$group': {
            '_id': {'year': '$year', 'season_type': '$season_type'},
            'totalFGM': {'$sum': {'$multiply': ['$FGM', '$GP']}},
            'totalFGA': {'$sum': {'$multiply': ['$FGA', '$GP']}},
            'countPlayers': {'$sum': 1}
        }
    },
    {
        '$project': {
            'season': '$_id',
            'averageFGM': {'$divide': ['$totalFGM', '$countPlayers']},
            'averageFGA': {'$divide': ['$totalFGA', '$countPlayers']},
        }
    },
    {
        '$project': {
            'season': 1,
            'averageFG%': {
                '$cond': {
                    'if': {'$eq': ['$averageFGA', 0]},
                    'then': None,
                    'else': {'$divide': ['$averageFGM', '$averageFGA']}
                }
            }
        }
    },
    {
        '$sort': {'averageFG%': -1}
    },
    {
        '$project': {
            '_id': 0,
            'year': '$season.year',
            'season_type': '$season.season_type',
            'averageFG%': 1
        }
    }
])

for result in results:
    print(result)

# Q9: Find the percentage of all points for each regular season and post season that came from the free throw line(i.e. FT indicates free throws made per game, where each free throw made is 1 point)
# and 2 point baskets(2PM is number of 2 pointers made per game) from highest to lowest percentage of points that come from free throws and 2 point baskets
print("----------------------------------")
print("Q9")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = statsCollection.aggregate([
    {
        '$group': {
            '_id': {
                'year': '$year',
                'season_type': '$season_type'
            },
            'total2PMPoints': {
                '$sum': {
                    '$multiply': ['$2PM', '$GP', 2] 
                }
            },
            'totalFTPoints': {
                '$sum': {
                    '$multiply': ['$FT', '$GP']
                }
            },
            'totalPoints': {
                '$sum': {
                    '$multiply': ['$PTS', '$GP']
                }
            }
        }
    },
    {
        '$project': {
            'season': '$_id',
            'percentageOfPointsFrom2PMAndFT': {
                '$cond': {
                    'if': {'$eq': ['$totalPoints', 0]},
                    'then': None,
                    'else': {
                        '$divide': [
                            {'$add': ['$total2PMPoints', '$totalFTPoints']},
                            '$totalPoints'
                        ]
                    }
                }
            }
        }
    },
    {
        '$sort': {
            'percentageOfPointsFrom2PMAndFT': -1
        }
    },
    {
        '$project': {
            '_id': 0,
            'year': '$season.year',
            'season_type': '$season.season_type',
            'percentageOfPointsFrom2PMAndFT': 1
        }
    }
])

for result in results:
    print(result)