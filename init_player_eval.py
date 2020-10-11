import pandas as pd
import numpy as np
import plotly_express
import glob

combine_data = pd.read_csv("~/Desktop/Python/NFL/ScrapedFiles/nfl_combine.csv")
salary_data = pd.read_csv("~/Desktop/Python/NFL/ScrapedFiles/nfl_salaries.csv")
draft_data = pd.read_csv("~/Desktop/Python/NFL/ScrapedFiles/nfl_draft.csv")

"""
git clone https://github.com/guga31bb/nflfastR-data
"""

# PLAY BY PLAY DATA #
path = '/Users/jasonrubenstein/Desktop/Python/NFL/nflfastR-data/data/*.csv.gz'
nfl_files = glob.glob(path)


class pbp_intro():

    def get_pbp_csvs(files):
        _full_data = pd.concat([
            pd.read_csv(datas,
                        dtype=str,
                        error_bad_lines=False,
                        delimiter=',',
                        compression='gzip')
            for datas in files], axis=0)
        df = _full_data.drop_duplicates(keep='first').reset_index(drop=True)
        return df

    def add_season(df):
        df['Season'] = 0

        df['game_date'] = pd.to_datetime(df['game_date'])

        df['Season'] = np.where(df['game_date'] > "2020-07-01", 2020, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2019-07-01") & (df['game_date'] < "2020-07-01"),
                                2019, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2018-07-01") & (df['game_date'] < "2019-07-01"),
                                2018, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2017-07-01") & (df['game_date'] < "2018-07-01"),
                                2017, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2016-07-01") & (df['game_date'] < "2017-07-01"),
                                2016, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2015-07-01") & (df['game_date'] < "2016-07-01"),
                                2015, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2014-07-01") & (df['game_date'] < "2015-07-01"),
                                2014, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2013-07-01") & (df['game_date'] < "2014-07-01"),
                                2013, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2012-07-01") & (df['game_date'] < "2013-07-01"),
                                2012, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2011-07-01") & (df['game_date'] < "2012-07-01"),
                                2011, df['Season'])
        df['Season'] = np.where((df['game_date'] > "2010-07-01") & (df['game_date'] < "2011-07-01"),
                                2010, df['Season'])

        df = df[(df['Season'] > 2009)]
        return df


_pbp_data = pbp_intro.get_pbp_csvs(nfl_files)


_pbp_data = pbp_intro.add_season(_pbp_data)


class NFL_analysis():
   
    def merge_draft_combine(combine_df, draft_df):
        combine_draft_merge = pd.merge(combine_df, draft_df,
                                       left_on=["Player", "Year"], right_on=["Player", "DraftYear"],
                                       how='left')
        return combine_draft_merge
        
    def fix_players(df):
        df['rusher_id'] = np.where(df['play_type'] == "Sack",
                                   df['passer_id'],
                                   df['rusher_id'])
        df['rusher_player_name'] = np.where(df['play_type'] == "Sack",
                                            df['passer_player_name'],
                                            df['rusher_player_name'])
        df['game_date'] = pd.to_datetime(df['game_date'])
        return df

    # Add model vars, pretty explanatory
    def add_model_variables(df):
        # df = fix_players(df)
        df['Shotgun_Ind'] = 0
        df['No_Huddle_Ind'] = 0
        # df['Team_Side_Gap'] = 0
        df['Incomplete_Pass'] = 0
        # df.loc[df['desc'].str.contains('Shotgun', case=False), 'Shotgun_Ind'] = 1
        # df.loc[df['desc'].str.contains('No Huddle', case=False), 'No_Huddle_Ind'] = 1
        df["reception"] = df['yards_after_catch'].isnull().map({True: 0, False: 1})
        df.loc[df['desc'].str.contains('incomplete', case=False), 'Incomplete_Pass'] = 1
        df['Home_Ind'] = np.where(df['posteam'] == df['home_team'], 1, 0)
        df['airEPA_Result'] = np.where(df['reception'] == 1, df['air_epa'], df['epa'])
        df['airWPA_Result'] = np.where(df['reception'] == 1, df['air_wpa'], df['wpa'])
        df['yacEPA_Result'] = np.where(df['reception'] == 1, df['yac_epa'], df['epa'])
        df['yacWPA_Result'] = np.where(df['reception'] == 1, df['yac_wpa'], df['wpa'])
        # df['RunGap'] = np.where(df['RunLocation'] == "middle", "center", df['RunGap'])
        # df['Team_Side_Gap'] = df['posteam'] + "-" + df['RunLocation'] + "-" + df['RunGap']
        return df

    # Data segregation / Add salary cols into this code want to group performance by season and match player pay to that
    def prep_pbp_data(df):
        df = df[df['qb_dropback'].notna()]
        df['epa'] = pd.to_numeric(df['epa'])
        df['wpa'] = pd.to_numeric(df['wpa'])
        # Passing
        pass_df = df[(df['qb_dropback'] == "1") & (df['passer_player_name'].notna())]
        cols_pass = [  # "airEPA_Result", "airWPA_Result", "yacEPA_Result", "yacWPA_Result",
            "passer_id", "receiver_id", "passer_player_name",  # "Passer_salary", "Passer_capHit", "PasserName",
            "receiver_player_name"  # , "Receiver_salary", "Receiver_capHit", "Receiver_position"]
        ]
        for i in cols_pass:
            pass_df = pass_df[pd.notnull(pass_df[i])]
        pass_df = pass_df[(pass_df['receiver_id'] != "None") & (pass_df['passer_id'] != "None")]
        # Rushing
        rush_df = df[(df['qb_dropback'] == "0")]
        cols_rush = ["epa", "wpa", "rusher_player_name", "rusher_id",  # "Rusher_capHit", "Rusher_salary"]
                     ]
        for k in cols_rush:
            rush_df = rush_df[pd.notnull(rush_df[k])]
        rush_df = rush_df[(rush_df['rusher_id'] != "None")]
        # Receiving
        rec_df = pass_df
        cols_rec = [  # "airEPA_Result", "airWPA_Result", "yacEPA_Result", "yacWPA_Result",
            "passer_id", "receiver_id", "receiver_player_name",  # "Passer_salary", "Passer_capHit",
            "passer_player_name"
            # , "Receiver_salary", "Receiver_capHit", "Receiver_position"]
        ]
        for g in cols_rec:
            rec_df = rec_df[pd.notnull(rec_df[g])]
        rec_df = rec_df[(rec_df['receiver_id'] != "None") & (rec_df['passer_id'] != "None")]
        # Team Passing
        team_passing = pass_df.groupby(['Season', 'posteam']).agg({
            'epa': sum,
            'wpa': sum,
            'play_id': 'count'
        }).reset_index()
        team_passing.rename(columns={'play_id': 'Pass_Attempts',
                                     'epa': 'Pass_EPA',
                                     'wpa': 'Pass_WPA'}, inplace=True)
        team_passing['Pass_EPA_Att'] = team_passing['Pass_EPA'] / team_passing['Pass_Attempts']
        team_passing['Pass_WPA_Att'] = team_passing['Pass_WPA'] / team_passing['Pass_Attempts']
        # Team Rushing
        team_rushing = rush_df.groupby(['Season', 'posteam']).agg({
            'epa': sum,
            'wpa': sum,
            'play_id': 'count'
        }).reset_index()
        team_rushing.rename(columns={'play_id': 'Rush_Attempts',
                                     'epa': 'Rush_EPA',
                                     'wpa': 'Rush_WPA'}, inplace=True)
        team_rushing['Rush_EPA_Att'] = team_rushing['Rush_EPA'] / team_rushing['Rush_Attempts']
        team_rushing['Rush_WPA_Att'] = team_rushing['Rush_WPA'] / team_rushing['Rush_Attempts']
        # Ind Passing
        ind_passing = pass_df.groupby(['Season', 'passer_player_name', 'posteam']).agg({
            'epa': sum,
            'wpa': sum,
            # 'airEPA_Result': sum,
            'play_id': 'count',
            # 'Passer_salary': 'max',
            # 'Passer_capHit': 'max'
        }).reset_index()
        ind_passing.rename(columns={'play_id': 'Pass_Attempts',
                                    'epa': 'Pass_EPA',
                                    'wpa': 'Pass_WPA'}, inplace=True)
        ind_passing['Pass_EPA_Att'] = ind_passing['Pass_EPA'] / ind_passing['Pass_Attempts']
        ind_passing['Pass_WPA_Att'] = ind_passing['Pass_WPA'] / ind_passing['Pass_Attempts']
        # ind_passing['airEPA_Att'] = ind_passing['airEPA_Result'] / ind_passing['Pass_Attempts']
        ind_passing = ind_passing[(ind_passing['Pass_Attempts'] > 10)]
        # Ind Rushing
        ind_rushing = rush_df.groupby(['Season', 'rusher_player_name', 'posteam']).agg({
            'epa': sum,
            'wpa': sum,
            'play_id': 'count',
            # 'Rusher_salary': 'max',
            # 'Rusher_capHit': 'max'
        }).reset_index()
        ind_rushing.rename(columns={'play_id': 'Rush_Attempts',
                                    'epa': 'Rush_EPA',
                                    'wpa': 'Rush_WPA',
                                    'rusher_player_name': 'Player'}, inplace=True)
        ind_rushing['Rush_EPA_Att'] = ind_rushing['Rush_EPA'] / ind_rushing['Rush_Attempts']
        ind_rushing['Rush_WPA_Att'] = ind_rushing['Rush_WPA'] / ind_rushing['Rush_Attempts']
        # ind_rushing = ind_rushing[(ind_rushing['Rush_Attempts'] > 25)]
        # filter out QBs from Rush df
        qbs = ind_passing[(ind_passing['Pass_Attempts'] > 15)]['passer_player_name']
        ind_rushing = ind_rushing[~ind_rushing['Player'].isin(qbs)]
        # Ind Receiving
        ind_receiving = rec_df.groupby(['Season', 'receiver_player_name', 'posteam']).agg({
            'epa': sum,
            'wpa': sum,
            'play_id': 'count',
            # 'Receiver_salary': 'max',
            # 'Receiver_capHit': 'max'
        }).reset_index()
        ind_receiving.rename(columns={'play_id': 'Targets',
                                      'epa': 'Rec_EPA',
                                      'wpa': 'Rec_WPA',
                                      'Receiver': 'Player'}, inplace=True)
        ind_receiving['Rec_EPA_Target'] = ind_receiving['Rec_EPA'] / ind_receiving['Targets']
        ind_receiving['Rec_WPA_Target'] = ind_receiving['Rec_WPA'] / ind_receiving['Targets']
        # ind_receiving = ind_receiving[(ind_receiving['Targets'] > 25)]
        # Combine ind_rushing and ind_receiving
        merged_ind = pd.merge(ind_rushing, ind_receiving,
                              left_on=["Season", "Player", "posteam"],
                              right_on=["Season", "receiver_player_name", "posteam"])
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
_pbp_data['posteam'] = _pbp_data['posteam'].map(team_name_dict)
_pbp_data['home_team'] = _pbp_data['home_team'].map(team_name_dict)
_pbp_data['away_team'] = _pbp_data['away_team'].map(team_name_dict)



combine_draft_merge = NFL_analysis.merge_draft_combine(combine_data, draft_data)


_pbp_data = NFL_analysis.fix_players(_pbp_data)
_pbp_data = NFL_analysis.add_model_variables(_pbp_data)

merged_team, team_pass_df, team_rush_df,\
ind_pass_df, ind_rush_df, ind_rec_df, ind_rec_rush_df = NFL_analysis.prep_pbp_data(_pbp_data)


## Plotting Example
min_att = ind_rec_df[(ind_rec_df['Targets'] > 15) & (ind_rec_df['Season'] == 2020)]
fig = plotly_express.scatter(min_att, x="Rec_EPA_Target", y="Rec_WPA_Target", color="receiver_player_name",
                             size='Targets',
                             hover_data=['posteam', 'Targets'])
fig.show()

