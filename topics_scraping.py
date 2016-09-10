import time
from bs4 import BeautifulSoup
import requests
from pandas import DataFrame, Series
import pandas as pd
import os
import re
from selenium import webdriver
#from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
from threading import Thread,BoundedSemaphore
import threading
# from multiprocessing import Pool,cpu_count
# from functools import partial

df_topics = DataFrame(columns=('Competition','Topic','URL','By','Views','Replies','Score'))
df_comp_detailed = DataFrame.from_csv('competitions_detailed.csv',encoding = 'utf-8')
root = 'https://www.kaggle.com'
topic_index = 0
Topic_Num = 0
#pool_data = BoundedSemaphore(value=8)
pool_html = BoundedSemaphore(value=16)

def get_html(url,file_name):
	global pool_html
	global Topic_Num
	pool_html.acquire()

	#if not os.path.exists(file_name+'.html'):
	#f = open(file_name+'.html','wb')
	
	try:
		html = requests.get(url).content
		html = html.replace(b"src=\"//", b"src=\"https://")
		html = html.replace(b"src=\"/", ("src=\""+"https://www.kaggle.com"+"/").encode())    
		html = html.replace(b"href=\"//", b"href=\"https://")
		html = html.replace(b"href=\"/", ("href=\""+"https://www.kaggle.com"+"/").encode())
		
		#f.write(html)
		Topic_Num += 1
	except:
		print(url+' page not available')
	
	#f.close()
	if Topic_Num%1000 == 0:
		print('********************************************',Topic_Num, url)
	print(url+' finished')

	pool_html.release()

def get_data_single_page(page_url,comp):
	html = requests.get(page_url).content
	soup = BeautifulSoup(html,'lxml')
	global topic_index
	global df_topics

	# for elem in soup.find(id='topiclist').find_all("div", {'class':'pinned topiclist-topic'}):
	# 	topic_url = elem.find('h3').find('a').attrs['href']
	# 	topic_title = elem.find('h3').find('a').get_text().replace("\r","").replace("\n","") 
	# 	topic_by = elem.find('h4').find('span').find('a').get_text()
	# 	topic_views = elem.find('div',{'class':"topiclist-topic-views views-col"}).get_text().replace("\r","").replace("\n","") 
	# 	topic_replies = elem.find('div',{'class':"topiclist-topic-replies replies-col"}).get_text().replace("\r","").replace("\n","") 
	# 	topic_score = elem.find('div',{'class':'topiclist-topic-score score-col'}).get_text().replace("\r","").replace("\n","") 
	# 	df_topics.loc[topic_index] = [comp,topic_title,root+topic_url,topic_by,topic_views,topic_replies,topic_score]
	# 	topic_index += 1

	for elem in soup.find(id='topiclist').find_all("div", {'class':'topiclist-topic'}):
		topic_url = elem.find('h3').find('a').attrs['href']
		topic_title = elem.find('h3').find('a').get_text().replace("\r","").replace("\n","").replace("  ","") 
		topic_by = elem.find('h4').find('span').find('a').get_text()
		topic_views = elem.find('div',{'class':"topiclist-topic-views views-col"}).get_text().replace("\r","").replace("\n","") 
		topic_replies = elem.find('div',{'class':"topiclist-topic-replies replies-col"}).get_text().replace("\r","").replace("\n","") 
		topic_score = elem.find('div',{'class':'topiclist-topic-score score-col'}).get_text().replace("\r","").replace("\n","") 
		df_topics.loc[topic_index] = [comp,topic_title,root+topic_url,topic_by,topic_views,topic_replies,topic_score]
		topic_index += 1

def get_data(url,comp):
	#global pool_data
	#pool_data.acquire()
	try:
		html = requests.get(url).content
		soup = BeautifulSoup(html,'lxml')
	except:
		print (url+' '+'not available')
		
	try:
		maxPage = 1
		for x in soup.find('div',{'class':'forum-pages'}).find_all(href=re.compile("page=")):
			if x.get_text().isdigit():             
				page = int(x.get_text())
				if  page > maxPage:
					maxPage = page

		print(comp+' '+"Total Pages:"+ str(maxPage))
		pagesUrl = url+ "?page="
		for pageIndex in range(1, maxPage+1):
			get_data_single_page(pagesUrl+str(pageIndex),comp)
	except:
		try:
			get_data_single_page(url,comp)
			#print(comp+' 1 page')
		except:
			print(url+' page 404 not found')


		
	#pool_data.release()

def MakeDir(folder):
	if not os.path.exists(folder):
		os.makedirs(folder)


def DataScraping():

	global df_comp_detailed
	global df_topics

	for ob in range(len(df_comp_detailed)):
	
		get_data(df_comp_detailed.URL.ix[ob]+'/forums',
						df_comp_detailed.ix[ob].Competition)
			
	df_topics.to_csv("Topics.csv", encoding = 'utf-8')
	print (df_topics)

def HtmlScraping():

	#global df_topics
	workers = []
	df_topics = DataFrame.from_csv('Topics.csv',encoding = 'utf-8')
	for i in range(len(df_topics)):
		MakeDir("Topics/"+str(df_topics.ix[i].Competition)+'/'+str(df_topics.Topic[i]))

	for ob in range(len(df_topics)):
		page_num = int(int(df_topics.Replies.ix[ob])/20) + 1
		for i in range(1,page_num+1):
			worker=Thread(target=get_html,args=(df_topics.URL.ix[ob]+'?page='+str(i),
						"Topics/"+str(df_topics.ix[ob].Competition)+'/'+str(df_topics.Topic[ob])+'/PAGE'+str(i),))

			#workers.append(worker)
			#worker.setDaemon(False)
			#if ob%500 == 0 and ob > 0:
				#print('sleeping')
				#time.sleep(300)
				
			#worker.start()
			#for w in workers:
			#worker.setDaemon(True)
			worker.start()
			if threading.active_count() > 1900:
				time.sleep(60)

			#worker.join()

if __name__ == "__main__":
	#DataScraping()
	HtmlScraping()
	



