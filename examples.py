"""
Practice mongoDB queries from nba_db collection
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

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)


# Q2: Retrieve the name, team, year, and points per game(PTS) of players with the top 3 points per game (PTS) in the regular season of a year from 2019 and onwards
# Note: Not necessary to include all ties
# Note: regular season implies season_type field is Regular Season
print("----------------------------------")
print("Q2")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q3: Retrieve the team and number of players who played in the post season for each time from 2020 and onwards from the most to the least number of players
# Note: This implies that season_type field is Post Season
print("----------------------------------")
print("Q3")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q4: Retrieve the player names and their total number of points(round to nearest integer) for all the players that played on the Los Angeles Lakers in the 2021 regular season from most points to least points
# Note: GP is games played and PTS is points per game
print("----------------------------------")
print("Q4")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q5: Retrieve the player names, year, and teams for players that played for more than two teams in the same year from 2021 and onwards with name and year sorted in ascending order
print("----------------------------------")
print("Q5")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q6: Retrieve player name and position of players that played in the 2021 and 2023 post seasons but not in the 2022 post season with name sorted in descending order
print("----------------------------------")
print("Q6")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q7: Retrieve the player name, points per game and number of turnovers per game over the 2019-2022 regular season of players that average more than 3 turnovers a game over those seasons sorting the turnovers per game in descending order
# Note: Do not take the average of the turnovers(TOV) over those seasons as the players may have not played the same number of games played
# Note: Only include players who played in all 4 regular seasons
print("----------------------------------")
print("Q7")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q8: Retrieve the average field goal percentage for each regular season and postseason from the highest to lowest average field goal percentage
# Note: Field goal percentage is calculated by FGM/FGA, DO NOT USE the average of FG% as this does not account for the number of games each player plays
# Note: FGM is field goals made per game and FGA is field goals attempted per game
print("----------------------------------")
print("Q8")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)

# Q9: Find the percentage of all points for each regular season and post season that came from the free throw line(i.e. FT indicates free throws made per game, where each free throw made is 1 point)
# and 2 point baskets(2PM is number of 2 pointers made per game) from highest to lowest percentage of points that come from free throws and 2 point baskets
print("----------------------------------")
print("Q9")
print("----------------------------------")

### --- Write your query here after the = sign of the result variable --- #
results = 

for result in results:
    print(result)
