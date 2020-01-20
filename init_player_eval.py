"""
Used season_play_by_play in R to pull data, thanks to https://github.com/ryurko
Salaries from https://docs.google.com/spreadsheets/d/1rds8LOv8t8HqtnM-OLzSbrNpdEy_8vUYAP39swye57I/edit?usp=sharing
Further data manipulation is rewritten in Python and partially expanded with more to come
"""

import os
import pandas as pd
import numpy as np
import plotly
import plotly_express
from collections import Counter
import matplotlib.pyplot as plt

os.environ["MODIN_ENGINE"] = "dask"
import modin.pandas as pd_modin

# Update paths for your own machine
nfl_data_pbp = pd.read_csv('~/Desktop/R/NFL/NFL_pbp_data.csv')
nfl_data_salary = pd.read_csv("~/Desktop/Python/NFL/nfl salary - Sheet1.csv")

# Fix nan, LA to LAR, STL to LAR,
def fix_teams(df):
    df = df[pd.notnull(df['posteam'])]
    df['posteam'] = np.where((df['posteam'] == "JAC"),
                             "JAX",
                             df['posteam'])
    df['posteam'] = np.where((df['posteam'] == "STL") | (df['posteam'] == "LA"),
                             "LAR",
                             df['posteam'])
    df['posteam'] = np.where((df['posteam'] == "SD"),
                             "LAC",
                             df['posteam'])
    df['DefensiveTeam'] = np.where((df['DefensiveTeam'] == "JAC"),
                                   "JAX",
                                   df['DefensiveTeam'])
    df['DefensiveTeam'] = np.where((df['DefensiveTeam'] == "STL") | (df['DefensiveTeam'] == "LA"),
                                   "LA",
                                   df['DefensiveTeam'])
    df['DefensiveTeam'] = np.where((df['DefensiveTeam'] == "SD"),
                                   "LAC",
                                   df['DefensiveTeam'])
    return df

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

# Run functions
df2 = fix_teams(nfl_data_pbp)
df3 = fix_names(df2)
df4 = fix_players(df3)
pbp_df = add_model_variables(df4)

# Merge salaries with pbp data
def fix_salaries(df):
    df = df.drop_duplicates(subset=['playerName', 'year'], keep='first')
    df = df[['playerName', 'year', 'team', 'salary', 'signingBonus', 'totalCash']]
    df['FirstLetter'] = df['playerName'].astype(str).str[0]
    df['FirstName'], df['LastName'] = df['playerName'].str.split(' ', 1).str
    df['matchName'] = df['FirstLetter'] + "." + df['LastName']
    # Delete redundant cols
    del (df['FirstLetter'], df['FirstName'], df['LastName'])

    # print(len(nfl_data_pbp))
    # Passer Salary, Rusher Salary, Receiver Salary add
    df.rename(columns={'year': 'Season',
                       'matchName': 'Passer',
                       'playerName': 'PasserName',
                       'salary': 'Passer_salary',
                       'signingBonus': 'Passer_signingBonus',
                       'totalCash': 'Passer_totalCash'}, inplace=True)
    pbp_merged_pass = pd.merge(pbp_df, df, on=["Season", "Passer"], how='left')
    # print(len(pbp_merged_pass))

    # Rusher
    df.rename(columns={'Passer': 'Rusher',
                       'PasserName': 'RusherName',
                       'Passer_salary': 'Rusher_salary',
                       'Passer_signingBonus': 'Rusher_signingBonus',
                       'Passer_totalCash': 'Rusher_totalCash'}, inplace=True)
    pbp_merged_pass_rush = pd.merge(pbp_merged_pass, df, on=["Season", "Rusher"], how='left')
    # print(len(pbp_merged_pass_rush))

    # Receiver
    df.rename(columns={'Rusher': 'Receiver',
                       'RusherName': 'ReceiverName',
                       'Rusher_salary': 'Receiver_salary',
                       'Rusher_signingBonus': 'Receiver_signingBonus',
                       'Rusher_totalCash': 'Receiver_totalCash'}, inplace=True)
    pbp_merged_salary = pd.merge(pbp_merged_pass_rush, df, on=["Season", "Receiver"], how='left')
    return pbp_merged_salary

# Data segregation / Add salary cols into this code want to group performance by season and match player pay to that
def prep_pbp_data(df):
    # Passing
    pass_df = df[(df['PlayType'] == "Pass")]
    cols_pass = ["airEPA_Result", "airWPA_Result", "yacEPA_Result", "yacWPA_Result", "PassLocation",
                 "Passer_ID", "Receiver_ID", "Passer", "Receiver"]
    for i in cols_pass:
        pass_df = pass_df[pd.notnull(pass_df[i])]
    pass_df = pass_df[(pass_df['Receiver_ID'] != "None") & (pass_df['Passer_ID'] != "None")]
    # Rushing
    rush_df = df[(df['PlayType'] == "Run") | (df['PlayType'] == "Sack")]
    cols_rush = ["EPA", "WPA", "Team_Side_Gap", "Rusher", "Rusher_ID"]
    for k in cols_rush:
        rush_df = rush_df[pd.notnull(rush_df[k])]
    rush_df = rush_df[(rush_df['Rusher_ID'] != "None")]
    # Receiving
    rec_df = pass_df
    cols_rec = ["airEPA_Result", "airWPA_Result", "yacEPA_Result", "yacWPA_Result", "PassLocation",
                "Passer_ID", "Receiver_ID", "Passer", "Receiver"]
    for g in cols_rec:
        rec_df = rec_df[pd.notnull(rec_df[g])]
    rec_df = rec_df[(rec_df['Receiver_ID'] != "None") & (rec_df['Passer_ID'] != "None")]
    # Team Passing
    team_passing = pass_df.groupby('posteam').agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    team_passing.rename(columns={'play_id': 'Pass_Attempts',
                                 'EPA': 'Pass_EPA',
                                 'WPA': 'Pass_WPA'}, inplace=True)
    team_passing['Pass_EPA_Att'] = team_passing['Pass_EPA'] / team_passing['Pass_Attempts']
    team_passing['Pass_WPA_Att'] = team_passing['Pass_WPA'] / team_passing['Pass_Attempts']
    # Team Rushing
    team_rushing = rush_df.groupby('posteam').agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    team_rushing.rename(columns={'play_id': 'Rush_Attempts',
                                 'EPA': 'Rush_EPA',
                                 'WPA': 'Rush_WPA'}, inplace=True)
    team_rushing['Rush_EPA_Att'] = team_rushing['Rush_EPA'] / team_rushing['Rush_Attempts']
    team_rushing['Rush_WPA_Att'] = team_rushing['Rush_WPA'] / team_rushing['Rush_Attempts']
    # Ind Passing
    ind_passing = pass_df.groupby('Passer').agg({
        'EPA': sum,
        'WPA': sum,
        'airEPA_Result': sum,
        'play_id': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    ind_passing.rename(columns={'play_id': 'Pass_Attempts',
                                'EPA': 'Pass_EPA',
                                'WPA': 'Pass_WPA'}, inplace=True)
    ind_passing['Pass_EPA_Att'] = ind_passing['Pass_EPA'] / ind_passing['Pass_Attempts']
    ind_passing['Pass_WPA_Att'] = ind_passing['Pass_WPA'] / ind_passing['Pass_Attempts']
    ind_passing['airEPA_Att'] = ind_passing['airEPA_Result'] / ind_passing['Pass_Attempts']
    ind_passing = ind_passing[(ind_passing['Pass_Attempts'] > 150)]
    # Ind Rushing
    ind_rushing = rush_df.groupby('Rusher').agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count'
        # 'Customer Email': 'nunique'
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
    ind_receiving = rec_df.groupby('Receiver').agg({
        'EPA': sum,
        'WPA': sum,
        'play_id': 'count'
    }).reset_index()
    ind_receiving.rename(columns={'play_id': 'Targets',
                                  'EPA': 'Rec_EPA',
                                  'WPA': 'Rec_WPA',
                                  'Receiver': 'Player'}, inplace=True)
    ind_receiving['Rec_EPA_Target'] = ind_receiving['Rec_EPA'] / ind_receiving['Targets']
    ind_receiving['Rec_WPA_Target'] = ind_receiving['Rec_WPA'] / ind_receiving['Targets']
    # ind_receiving = ind_receiving[(ind_receiving['Targets'] > 25)]
    # Combine ind_rushing and ind_receiving
    merged_ind = pd.merge(ind_rushing, ind_receiving, on="Player")
    merged_ind['Opportunities'] = merged_ind['Rush_Attempts'] + merged_ind['Targets']
    merged_ind['Weighted_Rush_EPA'] = merged_ind['Rush_Attempts'] * merged_ind['Rush_EPA_Att']
    merged_ind['Weighted_Rush_WPA'] = merged_ind['Rush_Attempts'] * merged_ind['Rush_WPA_Att']
    merged_ind['Weighted_Target_EPA'] = merged_ind['Targets'] * merged_ind['Rec_EPA_Target']
    merged_ind['Weighted_Target_WPA'] = merged_ind['Targets'] * merged_ind['Rec_WPA_Target']
    merged_ind['Weighted_EPA_Opps'] = (merged_ind['Weighted_Rush_EPA'] + merged_ind['Weighted_Target_EPA']) \
                                    / merged_ind['Opportunities']
    merged_ind['Weighted_WPA_Opps'] = (merged_ind['Weighted_Rush_WPA'] + merged_ind['Weighted_Target_WPA']) \
                                    / merged_ind['Opportunities']
    merged_team = pd.merge(team_passing, team_rushing, on="posteam")
    return merged_team, team_passing, team_rushing, ind_passing, ind_rushing, ind_receiving, merged_ind

# Run more functions
pbp_merged_salary = fix_salaries(nfl_data_salary)

merged_team, team_pass_df, team_rush_df,\
ind_pass_df, ind_rush_df, ind_rec_df, ind_rec_rush_df = prep_pbp_data(pbp_merged_salary)


# Plot EPA/Attempt for Rush and Pass (x, y) with color/legend for team name
ind_rec_rush_df_use = ind_rec_rush_df[(ind_rec_rush_df['Opportunities'] > 50) & (ind_rec_rush_df['Rush_Attempts'] > 5)]
fig = plotly_express.scatter(merged_team, x="Pass_WPA_Att", y="Rush_WPA_Att", color="posteam",
                             size='Rush_Attempts', hover_data=['posteam'])
fig.show()
