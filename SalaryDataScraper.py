# Still buggy, working to resolve

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


years = [2014]#, 2015, 2016, 2017, 2018, 2019]
cats = ['cash']  # , 'cap-hit', 'signing-cash', 'average']
urls = []
appended_data = pd.DataFrame()

for c_year in years:
    for c_cat in cats:
        url = "https://www.spotrac.com/nfl/rankings/" + str(c_year) + "/" + str(c_cat)
        urls.append(url)
        for url in urls:
            # if i < len(years):
                driver = webdriver.Chrome()
                driver.get(url)
                response = requests.get(url).text
                soup = bs(response, 'html.parser')
                salary_df = pd.DataFrame({'Player': [item.text.strip() for item in
                                                     soup.find_all('a', {'class': 'team-name'})],
                                          # Make if statement here for column name, if not just cash
                                          'CashEarnings': [item.text.strip() for item in
                                                           soup.find_all('span', {'class': 'info'})],
                                          'Position': [item.text.strip() for item in
                                                       soup.find_all('span', {'class': 'rank-position'})]
                                          })
                salary_df['Year'] = c_year
                appended_data = appended_data.append(salary_df)
                appended_data['CashEarnings'] = appended_data['CashEarnings'].str.replace('$', '')
                appended_data['CashEarnings'] = appended_data['CashEarnings'].str.replace(',', '')
                appended_data['CashEarnings'] = appended_data['CashEarnings'].astype(int)
        driver.close()
