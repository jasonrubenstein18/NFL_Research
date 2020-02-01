"""
Used season_play_by_play in R to pull data, thanks to https://github.com/ryurko
Rosters also courtesy of ryurko
Further data manipulation is rewritten in Python and partially expanded with more to come
"""

import os
import pandas as pd
import numpy as np
import plotly
import plotly_express
from collections import Counter
import matplotlib.pyplot as plt
import glob
# import modin.pandas as pd_modin
# os.environ["MODIN_ENGINE"] = "dask"
pbp = pd.read_csv('~/Desktop/R/NFL/Data/NFL_pbp_data.csv')
combine_data = pd.read_csv("~/Desktop/Python/NFL/Data/nfl_combine.csv")
salary_data = pd.read_csv("~/Desktop/Python/NFL/Data/nfl_salaries.csv")
draft_data = pd.read_csv("~/Desktop/Python/NFL/Data/nfl_draft.csv")

# Merge roster data with pbp data
path = 'path/to/rosters/*.csv'
roster_files = glob.glob(path)


def get_rosters(rosters):
    players = pd.concat([
                         pd.read_csv(players,
                                     dtype=str,
                                     error_bad_lines=False,
                                     delimiter=',')
        for players in rosters], axis=0)
    df = players.drop_duplicates(keep='first').reset_index(drop=True)
    return df


rosters_raw = get_rosters(roster_files)


def merge_rosters_pbp(pbp_df, roster_df):
    # pbp_df = pbp
    # roster_df = rosters_raw
    # rosters have season, abbr_player_name, team, position, gsis_id for matching
    # rosters have full_player_name as new col.
    # pbp have season, Passer/Rusher/Receiver/Tackler1/Tackler2/Returner/BlockingPlayer/RecFumbPlayer/PenalizedPlayer
        # posteam/DefensiveTeam, (Passer/Rusher/Receiver), Passer_ID/Rusher_ID/Receiver_ID
    roster_df['season'] = roster_df['season'].astype(int)
    # Passer
    roster_df.rename(columns={'full_player_name': 'Passer_name',
                              'position': 'Passer_position'}, inplace=True)
    pbp_passer = pd.merge(pbp_df, roster_df,
                          left_on=["Season", "Passer", "posteam", "Passer_ID"],
                          right_on=["season", "abbr_player_name", "team", "gsis_id"],
                          how='left')
    # Delete added cols
    del pbp_passer['season'], pbp_passer['abbr_player_name'], pbp_passer['team'], \
        pbp_passer['gsis_id'], pbp_passer['season_type']
    # Rusher
    roster_df.rename(columns={'Passer_name': 'Rusher_name',
                              'Passer_position': 'Rusher_position'}, inplace=True)
    pbp_rusher = pd.merge(pbp_passer, roster_df,
                          left_on=["Season", "Rusher", "posteam", "Rusher_ID"],
                          right_on=["season", "abbr_player_name", "team", "gsis_id"],
                          how='left')
    # Delete added cols
    del pbp_rusher['season'], pbp_rusher['abbr_player_name'], pbp_rusher['team'], \
        pbp_rusher['gsis_id'], pbp_rusher['season_type']
    # Receiver
    roster_df.rename(columns={'Rusher_name': 'Receiver_name',
                              'Rusher_position': 'Receiver_position'}, inplace=True)
    pbp_receiver = pd.merge(pbp_rusher, roster_df,
                            left_on=["Season", "Receiver", "posteam", "Receiver_ID"],
                            right_on=["season", "abbr_player_name", "team", "gsis_id"],
                            how='left')
    # Delete added cols
    del pbp_receiver['season'], pbp_receiver['abbr_player_name'], pbp_receiver['team'], \
        pbp_receiver['gsis_id'], pbp_receiver['season_type']
    # Tackler1
    roster_df.rename(columns={'Receiver_name': 'Tackler1_name',
                              'Receiver_position': 'Tackler1_position'}, inplace=True)
    pbp_tackle1 = pd.merge(pbp_receiver, roster_df,
                           left_on=["Season", "Tackler1", "DefensiveTeam"],
                           right_on=["season", "abbr_player_name", "team"],
                           how='left')
    # Delete added cols
    del pbp_tackle1['season'], pbp_tackle1['abbr_player_name'], pbp_tackle1['team'], \
        pbp_tackle1['gsis_id'], pbp_tackle1['season_type']
    # Tackler2
    roster_df.rename(columns={'Tackler1_name': 'Tackler2_name',
                              'Tackler1_position': 'Tackler2_position'}, inplace=True)
    pbp_tackle2 = pd.merge(pbp_tackle1, roster_df,
                           left_on=["Season", "Tackler2", "DefensiveTeam"],
                           right_on=["season", "abbr_player_name", "team"],
                           how='left')
    # Delete added cols
    del pbp_tackle2['season'], pbp_tackle2['abbr_player_name'], pbp_tackle2['team'], \
        pbp_tackle2['gsis_id'], pbp_tackle2['season_type']
    # Returner
    roster_df.rename(columns={'Tackler2_name': 'Returner_name',
                              'Tackler2_position': 'Returner_position'}, inplace=True)
    pbp_returner = pd.merge(pbp_tackle2, roster_df,
                            left_on=["Season", "Returner", "posteam"],
                            right_on=["season", "abbr_player_name", "team"],
                            how='left')
    # Delete added cols
    del pbp_returner['season'], pbp_returner['abbr_player_name'], pbp_returner['team'], \
        pbp_returner['gsis_id'], pbp_returner['season_type']
    # Blocking Player
    roster_df.rename(columns={'Returner_name': 'Blocker_name',
                              'Returner_position': 'Blocker_position'}, inplace=True)
    pbp_blocker = pd.merge(pbp_returner, roster_df,
                           left_on=["Season", "BlockingPlayer", "posteam"],
                           right_on=["season", "abbr_player_name", "team"],
                           how='left')
    # Delete added cols
    del pbp_blocker['season'], pbp_blocker['abbr_player_name'], pbp_blocker['team'], \
        pbp_blocker['gsis_id'], pbp_blocker['season_type']
    # RecFumbPlayer
    roster_df.rename(columns={'Blocker_name': 'RecFumbPlayer_name',
                              'Blocker_position': 'RecFumbPlayer_position'}, inplace=True)
    pbp_def_recfumb = pd.merge(pbp_blocker, roster_df,
                               left_on=["Season", "RecFumbPlayer", "posteam"],
                               right_on=["season", "abbr_player_name", "team"],
                               how='left')
    # Delete added cols
    del pbp_def_recfumb['season'], pbp_def_recfumb['abbr_player_name'], pbp_def_recfumb['team'], \
        pbp_def_recfumb['gsis_id'], pbp_def_recfumb['season_type']
    # PenalizedPlayer (Offense)
    roster_df.rename(columns={'RecFumbPlayer_name': 'DefensivePenalizedPlayer_name',
                              'RecFumbPlayer_position': 'DefensivePenalizedPlayer_position'}, inplace=True)
    pbp_def_penalty = pd.merge(pbp_def_recfumb, roster_df,
                               left_on=["Season", "PenalizedPlayer", "DefensiveTeam"],
                               right_on=["season", "abbr_player_name", "team"],
                               how='left')
    # Delete added cols
    del pbp_def_penalty['season'], pbp_def_penalty['abbr_player_name'], pbp_def_penalty['team'], \
        pbp_def_penalty['gsis_id'], pbp_def_penalty['season_type']
    # PenalizedPlayer (Offense)
    # roster_df.rename(columns={'OffensivePenalizedPlayer_name': 'DefensivePenalizedPlayer_name',
    #                           'OffensivePenalizedPlayer_position': 'DefensivePenalizedPlayer_position'}, inplace=True)
    # pbp_off_penalty = pd.merge(pbp_def_penalty, roster_df,
    #                            left_on=["Season", "PenalizedPlayer", "DefensiveTeam"],
    #                            right_on=["season", "abbr_player_name", "team"],
    #                            how='left')
    # # Delete added cols
    # del pbp_off_penalty['season'], pbp_off_penalty['abbr_player_name'], pbp_off_penalty['team'], \
    #     pbp_off_penalty['gsis_id'], pbp_off_penalty['season_type']
    return pbp_def_penalty


pbp_data = merge_rosters_pbp(pbp, rosters_raw)


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
pbp['SideofField'] = pbp['SideofField'].map(team_name_dict)
pbp['posteam'] = pbp['posteam'].map(team_name_dict)
pbp['DefensiveTeam'] = pbp['DefensiveTeam'].map(team_name_dict)
pbp['PenalizedTeam'] = pbp['PenalizedTeam'].map(team_name_dict)
pbp['HomeTeam'] = pbp['HomeTeam'].map(team_name_dict)
pbp['AwayTeam'] = pbp['AwayTeam'].map(team_name_dict)
pbp['Timeout_Team'] = pbp['Timeout_Team'].map(team_name_dict)


# Fix incorrect passer, rusher, receiver names given IDs, expect outsize run time
# Correct instances where single ID has multiple names (problem exists for all positions)
def fix_names(df):
    # Fix which passer/rusher/receiver is assigned to the given ID
    rusher_ids = df['Rusher_ID'].unique()
    for i in rusher_ids:
        try:
            a = df[(df['Rusher_ID'] == i)]
            ac = a.groupby('Rusher')['Rusher'].agg({'count'}).reset_index()
            max_player = ac['Rusher'][ac['count'].idxmax()]
            df['Rusher'] = np.where(df['Rusher_ID'] == i,
                                    max_player,
                                    df['Rusher'])
        except ValueError:
            pass
    receiver_ids = df['Receiver_ID'].unique()
    for i in receiver_ids:
        try:
            a = df[(df['Receiver_ID'] == i)]
            ac = a.groupby('Receiver')['Receiver'].agg({'count'}).reset_index()
            max_player = ac['Receiver'][ac['count'].idxmax()]
            df['Receiver'] = np.where(df['Receiver_ID'] == i,
                                      max_player,
                                      df['Receiver'])
        except ValueError:
            pass
    passer_ids = df['Passer_ID'].unique()
    for i in passer_ids:
        try:
            a = df[(df['Passer_ID'] == i)]
            ac = a.groupby('Passer')['Passer'].agg({'count'}).reset_index()
            max_player = ac['Passer'][ac['count'].idxmax()]
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
def attach_salaries(salary_df, pbp_df):
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
    pbp_sal.rename(columns={'Receiver_position_y': 'Receiver_position',
                            'Rusher_position_y': 'Rusher_position',
                            'Passer_position_y': 'Passer_position'}, inplace=True)
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
    ind_passing = pass_df.groupby(['Season', 'Passer', 'PasserName', 'posteam']).agg({
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
    ind_rushing = rush_df.groupby(['Season', 'Rusher', 'RusherName', 'posteam']).agg({
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
    ind_receiving = rec_df.groupby(['Season', 'Receiver', 'ReceiverName', 'posteam']).agg({
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
pbp_salary_merge = attach_salaries(salary_data, pbp_data)

merged_team, team_pass_df, team_rush_df,\
ind_pass_df, ind_rush_df, ind_rec_df, ind_rec_rush_df = prep_pbp_data(pbp_salary_merge)


# To be used on single position (rusher or receiver)
def merge_frames(pbp_df, combine_draft):
    # Merge
    combine_draft.rename(columns={'Team': 'Drafted_Team'}, inplace=True)
    df_merged = pd.merge(pbp_df, combine_draft,
                         left_on=pbp_df.columns[2],
                         right_on="Player",
                         how='left')
    return df_merged


working_data = merge_frames(ind_pass_df, combine_draft_merge)


'''
To Do:

1) Fix duplicate players (by Name) in pbp, salaries, combine, and draft. 
    - Something to do with first team, initial year, school? TBD..
    - Only 60 duplicated player names to resolve (at least 2 uniques) since 2000
    - 127 rows of duplicate player names since 2000. Might require more individual cleaning
    - PbP from ScrapR, Salaries from OTC, and combine/draft from pro-football-ref
2) attach data sets by player name and year (also possibly team)
'''


# Plot EPA/Attempt for Rush and Pass (x, y) with color/legend for team name
#ind_rec_rush_df_use = ind_rec_rush_df[(ind_rec_rush_df['Opportunities'] > 50) & (ind_rec_rush_df['Rush_Attempts'] > 5)]
min_att = working_data[(working_data['Pass_Attempts'] > 250) & (working_data['40yd'].notnull())]
min_att['40yd_scaled'] = min_att['40yd']

i = 0
while i < 2:
    min_att['40yd_scaled'] = (((min_att['40yd_scaled'] - min(min_att['40yd_scaled'])) * (100 - 1))
                              / (max(min_att['40yd_scaled']))
                              - min(min_att['40yd_scaled'])) + 1
    i += 1

fig = plotly_express.scatter(min_att, x="Pass_EPA_Att", y="Pass_WPA_Att", color="Passer",
                             size='40yd_scaled',
                             hover_data=['Passer', 'Pass_Attempts', 'Season', '40yd', 'College', 'Passer_salary',
                                         'Passer_capHit', 'DraftRnd', 'DraftPick', 'ProBowlCount'])
fig.show()

# Work on team normalized EPA and WPA values, need player performance aside from general team value
team_performance = pbp_salary_merge.groupby(['posteam', 'Season']).agg({
    'EPA': sum,
    'WPA': sum,
    'play_id': 'count',
}).reset_index()
ac = team_performance.groupby('posteam')['posteam'].agg({'count'}).reset_index()

print(len(team_performance))
