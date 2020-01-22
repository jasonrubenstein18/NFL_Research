import pandas as pd
import bs4
from bs4 import BeautifulSoup as bs
import re
import urllib
from urllib.request import urlopen, Request
import requests
import csv
from csv import writer
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import numpy as np
import time
import plotly
import plotly_express

# Scrape ProFootballReference for NFL Draft data -- only pulling overall pick #, not round
years = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009,
         2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]

appended_draft_data = pd.DataFrame()

urls = []
for y in years:
    url = "https://www.pro-football-reference.com/years/" + str(y) + "/draft.htm"
    urls.append(url)

for url in urls:
    series_url = pd.Series(url)
    df_url = series_url.str.split('/', expand=True)
    df_url.columns = ['protocol', 'blank', 'path', 'fixed_years', 'year', 'file_name']
    driver = webdriver.Chrome()
    driver.implicitly_wait(30)
    driver.get(url)
    # response = requests.get(url).text
    response = requests.get(url).text
    soup = bs(response, "html.parser")
    rows = soup.find_all('td')
    df = pd.Series([item.text.strip() for item in rows])
    df_fix = pd.DataFrame(np.reshape(df.values, (df.shape[0] // 28, 28)),
                          columns=['Pick', 'Team', 'Player', 'Position', 'Age', 'LastPlayed', 'AllProCount', 'ProBowlCount',
                                   'YearsStarting', 'CareerValuePFR', 'TeamValuePFR', 'GamesPlayed', 'PassComp', 'PassAtt',
                                   'PassYds', 'PassTD', 'PassINT', 'RushAtt', 'RushYds', 'RushTD', 'Recs', 'RecYds',
                                   'RecTD', 'SoloTackles', 'DefINT', 'Sacks', 'College', 'CollegeStatsDummy'])
    df_fix['DraftYear'] = df_url['year'][0]
    appended_draft_data = appended_draft_data.append(df_fix, ignore_index=True)
    time.sleep(5)
    driver.close()

print(appended_draft_data)

def remove_supplemental(df):
    df = df.drop_duplicates(subset=['Pick', 'DraftYear'], keep='first').reset_index(drop=True)
    return df

nfl_draft_only = remove_supplemental(appended_draft_data)

