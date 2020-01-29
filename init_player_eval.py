"""
Used season_play_by_play in R to pull data, thanks to https://github.com/ryurko
Salaries from https://docs.google.com/spreadsheets/d/1rds8LOv8t8HqtnM-OLzSbrNpdEy_8vUYAP39swye57I/edit?usp=sharing
Some required data manipulation is rewritten in Python and expanded with more to come
"""

import os
import pandas as pd
import numpy as np
import plotly
import plotly_express
from collections import Counter
import matplotlib.pyplot as plt
# import modin.pandas as pd_modin
# os.environ["MODIN_ENGINE"] = "dask"
pbp_data = pd.read_csv('~/Desktop/R/NFL/Data/NFL_pbp_data.csv')
combine_data = pd.read_csv("~/Desktop/Python/NFL/Data/nfl_combine.csv")
salary_data = pd.read_csv("~/Desktop/Python/NFL/Data/nfl_salaries.csv")
draft_data = pd.read_csv("~/Desktop/Python/NFL/Data/nfl_draft.csv")


def merge_draft_combine(combine_df, draft_df):
    del combine_df['College'], combine_df['Unnamed: 0'], draft_df['Unnamed: 0']
    combine_df.rename(columns={'School': 'College'}, inplace=True)
    combine_draft_merge = pd.merge(combine_df, draft_df,
                                   left_on=["Player", "College", "Year"], right_on=["Player", "College", "DraftYear"],
                                   how='left')
    return combine_draft_merge


combine_draft_merge = merge_draft_combine(combine_data, draft_data)

# Define team name dictionary
team_name_dict = {'Giants': "NYG", 'Steelers': "PIT", 'Bears': "CHI", 'Saints': "NO",
                  'Rams': "LA", 'Packers': "GB", 'Broncos': "DEN", 'Falcons': "ATL",
                  'Chargers': "LAC", 'Lions': "DET", 'Ravens': "BAL", 'Patriots': "NE",
                  'Cardinals': "ARI", 'Cowboys': "DAL", 'Bengals': "CIN",
                  'Raiders': "OAK", 'Panthers': "CAR", 'Colts': "IND", 'Redskins': "WAS",
                  'Vikings': "MIN", 'Jaguars': "JAX", 'Dolphins': "MIA", 'Buccaneers': "TB",
                  'Chiefs': "KC", 'Titans': "TEN", 'Bills': "BUF", 'Jets': 'NYJ', '49ers': 'SF',
                  'Texans': "HOU", 'Eagles': "PHI", 'Browns': "CLE", 'Seahawks': "SEA", np.nan: np.nan,
                  'New York Jets': "NYJ", 'Seattle Seahawks': "SEA", 'Kansas City Chiefs': "KC",
                  'Carolina Panthers': "CAR", 'Washington Redskins': "WAS", 'Chicago Bears': "CHI",
                  'Jacksonville Jaguars': "JAX", 'Cleveland Browns': "CLE", 'Arizona Cardinals': "ARI",
                  'Cincinnati Bengals': "CIN", 'San Diego Chargers': "LAC", 'Green Bay Packers': "GB",
                  'Minnesota Vikings': "MIN", 'St. Louis Rams': "LA", 'New England Patriots': "NE",
                  'New York Giants': "NYG", 'New Orleans Saints': "NO", 'Tennessee Titans': "TEN",
                  'Pittsburgh Steelers': "PIT", 'Denver Broncos': "DEN", 'San Francisco 49ers': "SF",
                  'Atlanta Falcons': "ATL", 'Tampa Bay Buccaneers': "TB", 'Detroit Lions': "DET",
                  'Miami Dolphins': "MIA", 'Dallas Cowboys': "DAL", 'Buffalo Bills': "BUF",
                  'Philadelphia Eagles': "PHI", 'Oakland Raiders': "OAK", 'Indianapolis Colts': "IND",
                  'Baltimore Ravens': "BAL", 'Houston Texans': "HOU", 'Los Angeles Rams': "LA",
                  'Los Angeles Chargers': "LA", 'SFO': "SF", 'GNB': "GB", 'KAN': "KC", 'STL': "LA",
                  'NOR': "NO", 'SDG': "LAC", 'NWE': "NE", 'TAM': "TB", 'LAR': "LA", 'CLE': "CLE",
                  'WAS': "WAS", 'CIN': "CIN", 'BAL': "BAL", 'PHI': "PHI", 'ARI': "ARI", 'PIT': "PIT",
                  'CHI': "CHI", 'NYG': "NYG", 'NYJ': "NYJ", 'DEN': "DEN", 'OAK': "OAK", 'SEA': "SEA",
                  'DET': "DET", 'CAR': "CAR", 'MIN': "MIN", 'BUF': "BUF", 'IND': "IND", 'JAX': "JAX",
                  'TEN': "TEN", 'ATL': "ATL", 'DAL': "DAL", 'MIA': "MIA", 'HOU': "HOU", 'GB': "GB",
                  'NO': "NO", 'JAC': "JAX", 'KC': "KC", 'NE': "NE", 'TB': "TB", 'SF': "SF", 'LA': "LA", 'LAC': "LAC"
                  }

# Fix team names and abbreviations based on dictionary
salary_data['Team'] = salary_data['Team'].map(team_name_dict)
draft_data['Team'] = draft_data['Team'].map(team_name_dict)
combine_data['DraftedTeam'] = combine_data['DraftedTeam'].map(team_name_dict)
pbp_data['SideofField'] = pbp_data['SideofField'].map(team_name_dict)
pbp_data['posteam'] = pbp_data['posteam'].map(team_name_dict)
pbp_data['DefensiveTeam'] = pbp_data['DefensiveTeam'].map(team_name_dict)
pbp_data['PenalizedTeam'] = pbp_data['PenalizedTeam'].map(team_name_dict)
pbp_data['HomeTeam'] = pbp_data['HomeTeam'].map(team_name_dict)
pbp_data['AwayTeam'] = pbp_data['AwayTeam'].map(team_name_dict)
pbp_data['Timeout_Team'] = pbp_data['Timeout_Team'].map(team_name_dict)


# Fix incorrect passer, rusher, receiver names given IDs, expect outsize run time
# Correct instances where single ID has multiple names (problem exists for all positions)
def fix_names(df):
    rusher_ids = df['Rusher_ID'].unique()
    for i in rusher_ids:
        try:
            a = df[(df['Rusher_ID']) == i]
            ac = a.groupby('Rusher')['Rusher'].agg({'count'}).reset_index()
            max_player = ac['Rusher'][ac['count'].argmax()]
            df['Rusher'] = np.where(df['Rusher_ID'] == i,
                                    max_player,
                                    df['Rusher'])
        except ValueError:
            pass
    receiver_ids = df['Receiver_ID'].unique()
    for i in receiver_ids:
        try:
            a = df[(df['Receiver_ID']) == i]
            ac = a.groupby('Receiver')['Receiver'].agg({'count'}).reset_index()
            max_player = ac['Receiver'][ac['count'].argmax()]
            df['Receiver'] = np.where(df['Receiver_ID'] == i,
                                      max_player,
                                      df['Receiver'])
        except ValueError:
            pass
    passer_ids = df['Passer_ID'].unique()
    for i in passer_ids:
        try:
            a = df[(df['Passer_ID']) == i]
            ac = a.groupby('Passer')['Passer'].agg({'count'}).reset_index()
            max_player = ac['Passer'][ac['count'].argmax()]
            df['Passer'] = np.where(df['Passer_ID'] == i,
                                    max_player,
                                    df['Passer'])
        except ValueError:
            pass
    return df


# Adjust rusher given play type
def fix_players(df):
    df = fix_names(df)
    df['Rusher_ID'] = np.where(df['PlayType'] == "Sack",
                               df['Passer_ID'],
                               df['Rusher_ID'])
    df['Rusher'] = np.where(df['PlayType'] == "Sack",
                            df['Passer'],
                            df['Rusher'])
    df['Passer'] = np.where(df['Passer_ID'] == "00-0034857",
                            "J.Allen",
                            df['Passer'])
    df['Receiver'] = np.where(df['Receiver_ID'] == "00-0031235",
                              "O.Beckham",
                              df['Receiver'])
    df['Rusher'] = np.where(df['Rusher_ID'] == "00-0019596",
                            "T.Brady",
                            df['Rusher'])

    return df


# Add model vars, clearly obfuscating function name
def add_model_variables(df):
    df = fix_players(df)
    df['Shotgun_Ind'] = 0
    df['No_Huddle_Ind'] = 0
    df['Team_Side_Gap'] = 0
    df['Incomplete_Pass'] = 0
    df.loc[df['desc'].str.contains('Shotgun', case=False), 'Shotgun_Ind'] = 1
    df.loc[df['desc'].str.contains('No Huddle', case=False), 'No_Huddle_Ind'] = 1
    df.loc[df['desc'].str.contains('Incomplete', case=False), 'Incomplete_Pass'] = 1
    df['Home_Ind'] = np.where(df['posteam'] == df['HomeTeam'], 1, 0)
    df['airEPA_Result'] = np.where(df['Reception'] == 1, df['airEPA'], df['EPA'])
    df['airWPA_Result'] = np.where(df['Reception'] == 1, df['airWPA'], df['WPA'])
    df['yacEPA_Result'] = np.where(df['Reception'] == 1, df['yacEPA'], df['EPA'])
    df['yacWPA_Result'] = np.where(df['Reception'] == 1, df['yacWPA'], df['WPA'])
    df['RunGap'] = np.where(df['RunLocation'] == "middle", "center", df['RunGap'])
    df['Team_Side_Gap'] = df['posteam'] + "-" + df['RunLocation'] + "-" + df['RunGap']
    return df


# Merge salaries with pbp data
def fix_salaries(salary_df, pbp_df):
    pbp_df = add_model_variables(pbp_df)
    df = salary_df.drop_duplicates(subset=['Player', 'Year', 'Team', 'CapHit'], keep='first')
    df = df[['Player', 'Team', 'CapHit', 'Salary', 'Position', 'Year']]
    df['FirstLetter'] = df['Player'].astype(str).str[0]
    df['FirstName'], df['LastName'] = df['Player'].str.split(' ', 1).str
    df['matchName'] = df['FirstLetter'] + "." + df['LastName']
    # Delete redundant cols
    del (df['FirstLetter'], df['FirstName'], df['LastName'])

    # print(len(nfl_data_pbp))
    # Passer Salary, Rusher Salary, Receiver Salary add
    df.rename(columns={'Player': 'PasserName',
                       'Salary': 'Passer_salary',
                       'CapHit': 'Passer_capHit',
                       'Position': 'Passer_position'}, inplace=True)
    pbp_merged_pass = pd.merge(pbp_df, df,
                               left_on=["Season", "Passer", "posteam"], right_on=["Year", "matchName", "Team"],
                               how='left')
    # Rusher
    df.rename(columns={'PasserName': 'RusherName',
                       'Passer_salary': 'Rusher_salary',
                       'Passer_capHit': 'Rusher_capHit',
                       'Passer_position': 'Rusher_position'}, inplace=True)
    pbp_merged_pass_rush = pd.merge(pbp_merged_pass, df,
                                    left_on=["Season", "Rusher", "posteam"], right_on=["Year", "matchName", "Team"],
                                    how='left')
    # Receiver
    df.rename(columns={'RusherName': 'ReceiverName',
                       'Rusher_salary': 'Receiver_salary',
                       'Rusher_capHit': 'Receiver_capHit',
                       'Rusher_position': 'Receiver_position'}, inplace=True)
    pbp_sal = pd.merge(pbp_merged_pass_rush, df,
                       left_on=["Season", "Receiver", "posteam"], right_on=["Year", "matchName", "Team"],
                       how='left')
    del pbp_sal['Team_x'], pbp_sal['Team_y'], pbp_sal['Year_x'], pbp_sal['Year_y'], \
        pbp_sal['matchName_x'], pbp_sal['matchName_y'], pbp_sal['Year'], pbp_sal['matchName']
    return pbp_sal


# Data segregation / Add salary cols into this code want to group performance by season and match player pay to that
def prep_pbp_data(df):
    # Passing
    pass_df = df[(df['PlayType'] == "Pass")]
    cols_pass = ["airEPA_Result", "airWPA_Result", "yacEPA_Result", "yacWPA_Result", "PassLocation",
                 "Passer_ID", "Receiver_ID", "Passer", "Receiver", "Passer_salary", "Passer_capHit", "PasserName",
                 "ReceiverName", "Receiver_salary", "Receiver_capHit", "Receiver_position"]
    for i in cols_pass:
        pass_df = pass_df[pd.notnull(pass_df[i])]
    pass_df = pass_df[(pass_df['Receiver_ID'] != "None") & (pass_df['Passer_ID'] != "None")]
    # Rushing
    rush_df = df[(df['PlayType'] == "Run") | (df['PlayType'] == "Sack")]
    cols_rush = ["EPA", "WPA", "Team_Side_Gap", "Rusher", "Rusher_ID", "RusherName", "Rusher_capHit", "Rusher_salary",
                 "Rusher_position"]
    for k in cols_rush:
        rush_df = rush_df[pd.notnull(rush_df[k])]
    rush_df = rush_df[(rush_df['Rusher_ID'] != "None")]
    # Receiving
    rec_df = pass_df
    cols_rec = ["airEPA_Result", "airWPA_Result", "yacEPA_Result", "yacWPA_Result", "PassLocation",
                "Passer_ID", "Receiver_ID", "Passer", "Receiver", "Passer_salary", "Passer_capHit", "PasserName",
                "ReceiverName", "Receiver_salary", "Receiver_capHit", "Receiver_position"]
    for g in cols_rec:
        rec_df = rec_df[pd.notnull(rec_df[g])]
    rec_df = rec_df[(rec_df['Receiver_ID'] != "None") & (rec_df['Passer_ID'] != "None")]
    # Team Passing
    team_passing = pass_df.groupby(['Season', 'posteam']).agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count'
    }).reset_index()
    team_passing.rename(columns={'play_id': 'Pass_Attempts',
                                 'EPA': 'Pass_EPA',
                                 'WPA': 'Pass_WPA'}, inplace=True)
    team_passing['Pass_EPA_Att'] = team_passing['Pass_EPA'] / team_passing['Pass_Attempts']
    team_passing['Pass_WPA_Att'] = team_passing['Pass_WPA'] / team_passing['Pass_Attempts']
    # Team Rushing
    team_rushing = rush_df.groupby(['Season', 'posteam']).agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count'
    }).reset_index()
    team_rushing.rename(columns={'play_id': 'Rush_Attempts',
                                 'EPA': 'Rush_EPA',
                                 'WPA': 'Rush_WPA'}, inplace=True)
    team_rushing['Rush_EPA_Att'] = team_rushing['Rush_EPA'] / team_rushing['Rush_Attempts']
    team_rushing['Rush_WPA_Att'] = team_rushing['Rush_WPA'] / team_rushing['Rush_Attempts']
    # Ind Passing
    ind_passing = pass_df.groupby(['Season', 'Passer']).agg({
        'EPA': sum,
        'WPA': sum,
        'airEPA_Result': sum,
        'play_id': 'count',
        'Passer_salary': 'max',
        'Passer_capHit': 'max'
    }).reset_index()
    ind_passing.rename(columns={'play_id': 'Pass_Attempts',
                                'EPA': 'Pass_EPA',
                                'WPA': 'Pass_WPA'}, inplace=True)
    ind_passing['Pass_EPA_Att'] = ind_passing['Pass_EPA'] / ind_passing['Pass_Attempts']
    ind_passing['Pass_WPA_Att'] = ind_passing['Pass_WPA'] / ind_passing['Pass_Attempts']
    ind_passing['airEPA_Att'] = ind_passing['airEPA_Result'] / ind_passing['Pass_Attempts']
    ind_passing = ind_passing[(ind_passing['Pass_Attempts'] > 150)]
    # Ind Rushing
    ind_rushing = rush_df.groupby(['Season', 'Rusher']).agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count',
        'Rusher_salary': 'max',
        'Rusher_capHit': 'max'
    }).reset_index()
    ind_rushing.rename(columns={'play_id': 'Rush_Attempts',
                                'EPA': 'Rush_EPA',
                                'WPA': 'Rush_WPA',
                                'Rusher': 'Player'}, inplace=True)
    ind_rushing['Rush_EPA_Att'] = ind_rushing['Rush_EPA'] / ind_rushing['Rush_Attempts']
    ind_rushing['Rush_WPA_Att'] = ind_rushing['Rush_WPA'] / ind_rushing['Rush_Attempts']
    # ind_rushing = ind_rushing[(ind_rushing['Rush_Attempts'] > 25)]
        # filter out QBs from Rush df
    qbs = ind_passing[(ind_passing['Pass_Attempts'] > 15)]['Passer']
    ind_rushing = ind_rushing[~ind_rushing['Player'].isin(qbs)]
    # Ind Receiving
    ind_receiving = rec_df.groupby(['Season', 'Receiver']).agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count',
        'Receiver_salary': 'max',
        'Receiver_capHit': 'max'
    }).reset_index()
    ind_receiving.rename(columns={'play_id': 'Targets',
                                  'EPA': 'Rec_EPA',
                                  'WPA': 'Rec_WPA',
                                  'Receiver': 'Player'}, inplace=True)
    ind_receiving['Rec_EPA_Target'] = ind_receiving['Rec_EPA'] / ind_receiving['Targets']
    ind_receiving['Rec_WPA_Target'] = ind_receiving['Rec_WPA'] / ind_receiving['Targets']
    # ind_receiving = ind_receiving[(ind_receiving['Targets'] > 25)]
    # Combine ind_rushing and ind_receiving
    merged_ind = pd.merge(ind_rushing, ind_receiving, on=["Player", "Season"])
    merged_ind['Opportunities'] = merged_ind['Rush_Attempts'] + merged_ind['Targets']
    merged_ind['Weighted_Rush_EPA'] = merged_ind['Rush_Attempts'] * merged_ind['Rush_EPA_Att']
    merged_ind['Weighted_Rush_WPA'] = merged_ind['Rush_Attempts'] * merged_ind['Rush_WPA_Att']
    merged_ind['Weighted_Target_EPA'] = merged_ind['Targets'] * merged_ind['Rec_EPA_Target']
    merged_ind['Weighted_Target_WPA'] = merged_ind['Targets'] * merged_ind['Rec_WPA_Target']
    merged_ind['Weighted_EPA_Opps'] = (merged_ind['Weighted_Rush_EPA'] + merged_ind['Weighted_Target_EPA']) \
                                    / merged_ind['Opportunities']
    merged_ind['Weighted_WPA_Opps'] = (merged_ind['Weighted_Rush_WPA'] + merged_ind['Weighted_Target_WPA']) \
                                    / merged_ind['Opportunities']
    merged_team = pd.merge(team_passing, team_rushing, on=["posteam", "Season"])
    return merged_team, team_passing, team_rushing, ind_passing, ind_rushing, ind_receiving, merged_ind


# Run more functions
pbp_salary_merge = fix_salaries(salary_data, pbp_data)

merged_team, team_pass_df, team_rush_df,\
ind_pass_df, ind_rush_df, ind_rec_df, ind_rec_rush_df = prep_pbp_data(pbp_salary_merge)



# Plot EPA/Attempt for Rush and Pass (x, y) with color/legend for team name
ind_rec_rush_df_use = ind_rec_rush_df[(ind_rec_rush_df['Opportunities'] > 50) & (ind_rec_rush_df['Rush_Attempts'] > 5)]
fig = plotly_express.scatter(merged_team, x="Pass_WPA_Att", y="Rush_WPA_Att", color="posteam",
                             size='Rush_Attempts', hover_data=['posteam'])
fig.show()
