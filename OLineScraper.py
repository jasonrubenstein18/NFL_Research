import pandas as pd
import time

## scrape football outsiders o-line metrics
years = [2019, 2020]

appended_line_data = pd.DataFrame()

urls = []
for y in years:
    url = "https://www.footballoutsiders.com/stats/nfl/offensive-line/" + str(y) + ""
    urls.append(url)

for url in urls:
    series_url = pd.Series(url)
    df_url = series_url.str.split('/|-', expand=True)
    df_url.columns = ['protocol', 'blank', 'path_one', 'path_two', 'path_three', 'fixed', 'file_name', 'year']
    line_table = pd.read_html(url)[0]
    line_table['Year'] = df_url['year'][0]
    appended_line_data = appended_line_data.append(line_table, ignore_index=True)
    time.sleep(5)

appended_line_data.columns = ['Rush_Rank', 'Rush_Team', 'Adj_Line_Yards', 'RB_Yards', 'Power_Success', 'Power_Rank',
                              'Stuffed', 'Stuffed_Rank', '2nd_Level_Yards', '2nd_Level_Rank', 'Open_Field_Yards',
                              'Open_Field_Rank', 'Pass_Team', 'Pass_Rank', 'Sacks', 'Adjusted_Sack_Rate', 'Season']


pass_line_data = appended_line_data[["Pass_Team", "Pass_Rank", "Sacks", "Adjusted_Sack_Rate", "Season"]]
pass_line_data = pass_line_data[(pass_line_data['Pass_Team'] != "NFL")]
