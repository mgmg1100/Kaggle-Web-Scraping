from __future__ import absolute_import

from celery import Celery

import os
import time
import datetime
import multiprocessing

# for save webpage as local html file
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# for get page and user list
import re
import requests
from bs4 import BeautifulSoup
from lxml import etree

app = Celery('proj',
             broker='amqp://guest:guest@localhost:5672//',
             #backend='amqp',
             )

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_RESULT_BACKEND='amqp',
    CELERY_TASK_RESULT_EXPIRES=3600,	
)

# For each user found in this page, call save2html() 
def scrapeSinglePage(pageUrl):
    #global totalUsers
    #global root
    r = requests.get("https://www.kaggle.com/users?page=" + str(pageUrl))
    soup = BeautifulSoup(r.content,'lxml')
    # retrieve user link in one page
    for ul in soup.find_all("a", class_='profilelink'):
        userName = ul['href'][1:]
        print(userName)
        #save2html(userName)
        app.send_task('proj.task.save2html', args=(userName,))
        #totalUsers = totalUsers +1
    return

# Web scaper main function
# find all pages, then call scrapeSinglePage() for each page
def main_scrapeAllPages(url):
    #global totalUsers
    print("Start time:")
    print(datetime.datetime.now())
    
    # retrieve how many pages (every pages has at most 100 users)
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'lxml')    
    maxPage = 1
    for x in soup.find(id='user-pages').find_all(href=re.compile("page=")):
        if x.get_text().isdigit():             
            page = int(x.get_text())
            if  page > maxPage:
                maxPage = page
                
    print("Total Pages:"+ str(maxPage))
    # process all pages
    pagesUrl = url+ "?page="
    #maxPage =3 
    pool = multiprocessing.Pool() # use all available cores
#     for pageIndex in range(1, maxPage+1):
#         #scrapeSinglePage(pagesUrl+str(pageIndex))
#         pool.apply_async(scrapeSinglePage, args=(pagesUrl+str(pageIndex),))

    
    pool.map(scrapeSinglePage, range(1, maxPage+1))
        
    pool.close()
    pool.join()
    print("End time:")
    print(datetime.datetime.now())
    #print("Total processed user number:")
    #print(totalUsers)
    return    


# main function
if __name__ == "__main__":
    main_scrapeAllPages("https://www.kaggle.com/users")


