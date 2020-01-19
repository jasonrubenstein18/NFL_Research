"""
Used season_play_by_play in R to pull data, thanks to https://github.com/ryurko
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


nfl_data_pbp = pd.read_csv('~/Desktop/R/NFL/NFL_pbp_data.csv')
nfl_data_salary = pd.read_csv("~/Desktop/Python/NFL/nfl salary - Sheet1.csv")

print(list(nfl_data_pbp))

# Fix nan, LA to LAR, STL to LAR,
nfl_data_pbp = nfl_data_pbp[pd.notnull(nfl_data_pbp['posteam'])]
def fix_teams(df):
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
nfl_data_pbp = fix_teams(nfl_data_pbp)

print(nfl_data_pbp['Passer'].unique())


# active_grp = nfl_data_pbp.groupby('Passer_ID').agg({
#     'Passer': 'nunique'
# }).reset_index()
#
# active_grp_not1 = active_grp[active_grp['Passer'] != 1]
# agrp = nfl_data_pbp[nfl_data_pbp['Passer_ID'].isin(active_grp_not1['Passer_ID'])]

russ = nfl_data_pbp[(nfl_data_pbp['Passer_ID'] == "None")]
russ = nfl_data_pbp.loc[nfl_data_pbp['desc'].str.contains('Incomplete', case=False)]

def player_fixes(df):
    df['Rusher_ID'] = np.where(df['PlayType'] == "Sack",
                               df['Passer_ID'],
                               df['Rusher_ID'])
    df['Rusher'] = np.where(df['PlayType'] == "Sack",
                            df['Passer'],
                            df['Rusher'])
    return df

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


def prepare_data(df):
    df = df4
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
        'Date': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    team_passing.rename(columns={'Date': 'Pass_Attempts',
                                 'EPA': 'Pass_EPA',
                                 'WPA': 'Pass_WPA'}, inplace=True)
    team_passing['Pass_EPA_Att'] = team_passing['Pass_EPA'] / team_passing['Pass_Attempts']
    team_passing['Pass_WPA_Att'] = team_passing['Pass_WPA'] / team_passing['Pass_Attempts']
    # Team Rushing
    team_rushing = rush_df.groupby('posteam').agg({
        'EPA': sum,
        'WPA': sum,
        'Date': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    team_rushing.rename(columns={'Date': 'Rush_Attempts',
                                 'EPA': 'Rush_EPA',
                                 'WPA': 'Rush_WPA'}, inplace=True)
    team_rushing['Rush_EPA_Att'] = team_rushing['Rush_EPA'] / team_rushing['Rush_Attempts']
    team_rushing['Rush_WPA_Att'] = team_rushing['Rush_WPA'] / team_rushing['Rush_Attempts']
    # Ind Passing
    ind_passing = pass_df.groupby('Passer').agg({
        'EPA': sum,
        'WPA': sum,
        'Date': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    ind_passing.rename(columns={'Date': 'Pass_Attempts',
                                'EPA': 'Pass_EPA',
                                'WPA': 'Pass_WPA'}, inplace=True)
    ind_passing['Pass_EPA_Att'] = ind_passing['Pass_EPA'] / ind_passing['Pass_Attempts']
    ind_passing['Pass_WPA_Att'] = ind_passing['Pass_WPA'] / ind_passing['Pass_Attempts']
    ind_passing = ind_passing[(ind_passing['Pass_Attempts'] > 100)]
    # Ind Rushing
    ind_rushing = rush_df.groupby('Rusher').agg({
        'EPA': sum,
        'WPA': sum,
        'Date': 'count'
        # 'Customer Email': 'nunique'
    }).reset_index()
    ind_rushing.rename(columns={'Date': 'Rush_Attempts',
                                'EPA': 'Rush_EPA',
                                'WPA': 'Rush_WPA',
                                'Rusher': 'Player'}, inplace=True)
    ind_rushing['Rush_EPA_Att'] = ind_rushing['Rush_EPA'] / ind_rushing['Rush_Attempts']
    ind_rushing['Rush_WPA_Att'] = ind_rushing['Rush_WPA'] / ind_rushing['Rush_Attempts']
    ind_rushing = ind_rushing[(ind_rushing['Rush_Attempts'] > 100)]
        # filter out QBs from Rush df
    qbs = ind_passing[(ind_passing['Pass_Attempts'] > 15)]['Passer']
    ind_rushing = ind_rushing[~ind_rushing['Player'].isin(qbs)]
    # Ind Receiving
    ind_receiving = rec_df.groupby('Receiver').agg({
        'EPA': sum,
        'WPA': sum,
        'Date': 'count'
    }).reset_index()
    ind_receiving.rename(columns={'Date': 'Targets',
                                  'EPA': 'Rec_EPA',
                                  'WPA': 'Rec_WPA',
                                  'Receiver': 'Player'}, inplace=True)
    ind_receiving['Rec_EPA_Target'] = ind_receiving['Rec_EPA'] / ind_receiving['Targets']
    ind_receiving['Rec_WPA_Target'] = ind_receiving['Rec_WPA'] / ind_receiving['Targets']
    ind_receiving = ind_receiving[(ind_receiving['Targets'] > 30)]
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
    return team_passing, team_rushing, ind_passing, ind_rushing, ind_receiving, merged_ind


df2 = fix_teams(nfl_data_pbp)
df3 = player_fixes(df2)
df4 = add_model_variables(df3)
team_pass_df, team_rush_df, ind_pass_df, ind_rush_df, ind_rec_df, ind_rec_rush_df = prepare_data(df4)



merged_team = pd.merge(team_pass_df, team_rush_df, on="posteam")


# Plot EPA/Attempt for Rush and Pass (x, y) with color/legend for team name
fig = plotly_express.scatter(ind_rec_rush_df, x="Weighted_EPA_Opps", y="Weighted_WPA_Opps", color="Player",
                             size='Opportunities', hover_data=['Player'])
fig.show()




# def add_positions(df, years):
#     def find_player_names(player_names):
#         if len(df[player_names]) == 0:
#             result = "None"
#         else:
#             freq = df[player_names].value_counts()
#             result = freq.max()
#         return result
#
#     passer_names = df.groupby('Passer_ID').apply(find_player_names)
#     # receiver_names = df.groupby('Receiver_ID').agg({'Receiver_Name': find_player_names("Receiver")})
#     # rusher_names = df.groupby('Rusher_ID').agg({'Rusher_Name': find_player_names("Rusher")})
#
#     df = df.merge(passer_names, on='Passer_ID', how='left')
#     # df = df.merge(receiver_names, on='Receiver_ID', how='left')
#     # df = df.merge(rusher_names, on='Rusher_ID', how='left')
#     return df


