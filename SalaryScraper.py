import pandas as pd
import bs4
import re
import urllib
import os
import numpy as np
import time
import requests
import csv
from csv import writer
from urllib.request import urlopen, Request
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup as bs


## scrape overthecap yearly salary data and position
years = [2014, 2015, 2016, 2017, 2018, 2019]
positions = ['quarterback', 'running-back', 'wide-receiver', 'tight-end', 'left-tackle', 'left-guard',
             'center', 'right-guard', 'right-tackle', 'defensive-line', 'linebacker', 'defensive-back',
             'kicker', 'punter']


appended_data = pd.DataFrame()

urls = []
for y in years:
    for p in positions:
        url = "https://overthecap.com/position/" + str(p) + "/" + str(y) + ""
        urls.append(url)

for url in urls:
    series_url = pd.Series(url)
    df_url = series_url.str.split('/', expand=True)
    df_url.columns = ['protocol', 'blank', 'path', 'pos_fixed', 'position', 'year']
    driver = webdriver.Chrome()
    driver.implicitly_wait(30)
    driver.get(url)
    # response = requests.get(url).text
    response = requests.get(url).text
    soup = bs(response, "html.parser")
    rows = soup.find_all('td')
    df = pd.Series([item.text.strip() for item in rows])
    df_fix = pd.DataFrame(np.reshape(df.values, (df.shape[0] // 4, 4)),
                          columns=['Player', 'Team', 'CapHit', 'Salary'])
    df_fix['Year'] = df_url['year'][0]
    df_fix['Position'] = df_url['position'][0]
    appended_data = appended_data.append(df_fix, ignore_index=True)
    time.sleep(5)
    driver.close()

def cleanup_sal_data(df):
    df['CapHit'] = df['CapHit'].str.replace('$', '')
    df['CapHit'] = df['CapHit'].str.replace(',', '')
    df['Salary'] = df['Salary'].str.replace('$', '')
    df['Salary'] = df['Salary'].str.replace(',', '')
    df['CapHit'] = df['CapHit'].astype(int)
    df['Salary'] = df['Salary'].astype(int)
    return df

appended_data_full = cleanup_sal_data(appended_data)

appended_data_full.to_csv(...)
