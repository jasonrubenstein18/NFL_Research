import pandas as pd
import bs4
from bs4 import BeautifulSoup as bs
import urllib
import csv
from csv import writer
from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np
import time
import plotly
import plotly_express


# Scrape ProFootballReference for NFL Draft data -- only pulling overall pick #, not round
years = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009,
         2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]

appended_combine_data = pd.DataFrame()

urls = []
for y in years:
    url = "https://www.pro-football-reference.com/draft/" + str(y) + "-combine.htm"
    urls.append(url)

for url in urls:
    series_url = pd.Series(url)
    df_url = series_url.str.split('/|-', expand=True)
    df_url.columns = ['protocol', 'blank', 'path_one', 'path_two', 'path_three', 'draft_fixed', 'year', 'file_name']
    combine_table = pd.read_html(url)[0]
    combine_table['Year'] = df_url['year'][0]
    appended_combine_data = appended_combine_data.append(combine_table, ignore_index=True)
    # appended_combine_data = appended_combine_data[(appended_combine_data['Drafted (tm/rnd/yr)'] != "Drafted (tm/rnd/yr)")]
    time.sleep(5)

check_data = appended_combine_data


def clean_combine_data(df):
    df = df[(df['Player'] != "Player")]
    df['DraftedTeam'], df['DraftRnd'], df['DraftPick'], df['dummyYear'] = df['Drafted (tm/rnd/yr)'].\
        str.split(' / ', 4).str
    # Could be more efficient. Still just did it instance by instance for now
    df['DraftRnd'] = df['DraftRnd'].str.replace('st', '')
    df['DraftRnd'] = df['DraftRnd'].str.replace('nd', '')
    df['DraftRnd'] = df['DraftRnd'].str.replace('th', '')
    df['DraftRnd'] = df['DraftRnd'].str.replace('rd', '')
    df['DraftPick'] = df['DraftPick'].str.replace('st pick', '')
    df['DraftPick'] = df['DraftPick'].str.replace('nd pick', '')
    df['DraftPick'] = df['DraftPick'].str.replace('th pick', '')
    df['DraftPick'] = df['DraftPick'].str.replace('rd pick', '')
    df.fillna({'DraftedTeam': 'Undrafted',
               'DraftRnd': 'Undrafted',
               'DraftPick': 'Undrafted',
               '40yd': 'DNP',
               'Vertical': 'DNP',
               'Bench': 'DNP',
               'Broad Jump': 'DNP',
               '3Cone': 'DNP',
               'Shuttle': 'DNP',
               'Wt': 'NotMeasured',
               'Height': 'NotMeasured'}, inplace=True)
    df['HeightFt'], df['HeightIn'] = df['Ht'].str.split('-', 2).str
    df[["HeightFt", "HeightIn"]] = df[["HeightFt", "HeightIn"]].apply(pd.to_numeric)
    df['Height'] = df['HeightFt']*12 + df['HeightIn']
    del df['HeightFt'], df['HeightIn'], df['Ht'], df['Drafted (tm/rnd/yr)'], df['dummyYear']
    return df


clean_combine = clean_combine_data(check_data)


# print(appended_combine_data)
