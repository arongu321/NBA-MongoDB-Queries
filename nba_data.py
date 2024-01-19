import pandas as pd
import simplejson as json

def renameColumns(stats_table):
    renamedColumns = {
        'Player': 'name',
        'Pos': 'position',
        'Tm': 'team',
        'G': 'GP',
        'MP': 'MPG',
        'FG': 'FGM',
        '3P': '3PM',
        '2P': '2PM'
    }
    stats_table.rename(columns=renamedColumns, inplace=True)
    return stats_table

def mapTeams(stats_table):
    team_mapping = {
        'MEM': 'Memphis Grizzlies',
        'TOR': 'Toronto Raptors',
        'MIA': 'Miami Heat',
        'BOS': 'Boston Celtics',
        'CLE': 'Cleveland Cavaliers',
        'MIL': 'Milwaukee Bucks',
        'UTA': 'Utah Jazz',
        'LAL': 'Los Angeles Lakers',
        'NOP': 'New Orleans Pelicans',
        'IND': 'Indiana Pacers',
        'WAS': 'Washington Wizards',
        'HOU': 'Houston Rockets',
        'PHO': 'Phoenix Suns',
        'SAC': 'Sacramento Kings',
        'DET': 'Detroit Pistons',
        'CHO': 'Charlotte Hornets',
        'CHI': 'Chicago Bulls',
        'ORL': 'Orlando Magic',
        'OKC': 'Oklahoma City Thunder',
        'MIN': 'Minnesota Timberwolves',
        'BRK': 'Brooklyn Nets',
        'DAL': 'Dallas Mavericks',
        'GSW': 'Golden State Warriors', 
        'POR': 'Portland Trail Blazers',
        'ATL': 'Atlanta Hawks',
        'DEN': 'Denver Nuggets',
        'LAC': 'Los Angeles Clippers',
        'NYK': 'New York Knicks',
        'SAS': 'San Antonio Spurs',
        'PHI': 'Philadelphia 76ers'
    }
    stats_table['team'] = stats_table['team'].replace(team_mapping)
    return stats_table

def convertDataInt(stats_table):
    integerColumns = stats_table.columns[3:5]
    stats_table[integerColumns] = stats_table[integerColumns].astype(int)
    return stats_table
    
def convertDataFloat(stats_table):
    floatColumns = stats_table.columns[5:28]
    stats_table[floatColumns] = stats_table[floatColumns].astype(float)
    return stats_table

def removeRepHeaders(stats_table):
    # Filtering out rows where any of the values match the column names
    mask = (stats_table != stats_table.columns).all(axis=1)
    return stats_table[mask]

nbaData = []
url = 'https://www.basketball-reference.com/leagues/NBA_{}_per_game.html'
years = list(range(2018, 2024))

for year in years:
    table = pd.read_html(url.format(year))
    stats_table = table[0]
    stats_table = removeRepHeaders(stats_table)
    stats_table = stats_table.drop(['Rk', 'Age'], axis=1)
    stats_table['year'] = year
    stats_table['season_type'] = 'Regular Season'
    stats_table = stats_table[stats_table['Tm'] != 'TOT']
    stats_table = renameColumns(stats_table)
    stats_table = mapTeams(stats_table)
    stats_table = convertDataInt(stats_table)
    stats_table = convertDataFloat(stats_table)
    nbaData.append(stats_table)

playoffsURL = 'https://www.basketball-reference.com/playoffs/NBA_{}_per_game.html'
for year in years:
    table = pd.read_html(playoffsURL.format(year))
    stats_table = table[0]
    stats_table = removeRepHeaders(stats_table)
    stats_table = stats_table.drop(['Rk', 'Age'], axis=1)
    stats_table['year'] = year
    stats_table['season_type'] = 'Post Season'
    stats_table = stats_table[stats_table['Tm'] != 'TOT']
    stats_table = renameColumns(stats_table)
    stats_table = mapTeams(stats_table)
    stats_table = convertDataInt(stats_table)
    stats_table = convertDataFloat(stats_table)
    nbaData.append(stats_table)

# Concatenate all dataframes
all_data = pd.concat(nbaData, ignore_index=True)

# Convert the data to dictionary format with key as "zips"
formatted_data = {"player_stats": all_data.to_dict(orient='records')}

# Write to a JSON file
with open('nba_data.json', 'w') as file:
    file.write(json.dumps(formatted_data, ignore_nan=True, indent=4))