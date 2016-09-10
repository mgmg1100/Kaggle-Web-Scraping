import time
#from bs4 import BeautifulSoup
import requests
from pandas import DataFrame, Series
import pandas as pd
import os
#from urllib.request import urlopen
import urllib
from selenium import webdriver
#from selenium.webdriver.common.by import By
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support import expected_conditions as EC
from threading import Thread,BoundedSemaphore
# from multiprocessing import Pool,cpu_count

df_comp = DataFrame(columns=('Competition','Reward','URL','Status'))
df_comp_detailed = DataFrame(columns=('Competition','Reward','URL','Status','Description_Text',
							'Evaluation','Rules','Prizes','Data','Leaderboard'))
file_names = ["description","evaluation","rules","prizes","data","leaderboard"]
page = ["","/details/evaluation","/details/rules","/details/prizes","/data","/leaderboard"]

pool=BoundedSemaphore(value=8)

def get_html(url,file_name):
	global pool
	pool.acquire()
	if not os.path.exists(file_name+'.html'):
		f = open(file_name+'.html','wb')
	
	#browser.get(url)
	# try:
	# 	WebDriverWait(br, 10).until(EC.presence_of_element_located((By.XPATH, "//*[contains(@class, '_panel')]")))
	# except:
	# 	print(url+' page not available')
	# finally:
	# 	html = browser.page_source.encode('utf-8') #bytes
	try:
		html = requests.get(url).content
		html = html.replace(b"src=\"//", b"src=\"https://")
		html = html.replace(b"src=\"/", ("src=\""+"https://www.kaggle.com"+"/").encode())    
		html = html.replace(b"href=\"//", b"href=\"https://")
		html = html.replace(b"href=\"/", ("href=\""+"https://www.kaggle.com"+"/").encode())
	except:
		print(url+' page not available')
	f.write(html)
	f.close()
	print(url+' finished')   

	pool.release()

def MakeDir(folder):
	if not os.path.exists(folder):
		os.makedirs(folder)

def TextScraping(url):
	try:
		request = requests.get(url)
		soup = BeautifulSoup(request.content)
		text = ''
	
		for p in soup.findAll('p'):
			text += p.text
		return text
	except:
		return None

def DataScraping(url="https://www.kaggle.com/competitions",output_file="competitions_detailed.csv"):
	global df_comp
	global df_comp_detailed
	global page

	#opts = webdriver.ChromeOptrions()
	#opts.binary_location(value="")
	driver = webdriver.Chrome()
	driver.get(url)

	driver.find_element_by_css_selector("li#all-switch").click()
	time.sleep(3)
	driver.find_element_by_css_selector("label.active[for='completed']").click()
	time.sleep(3)

	# comp_finisehd = driver.find_elements_by_xpath("//tr[@class='finished-comp']//div[@class='competition-details']/a ")
	# finished_reward = driver.find_elements_by_xpath("//tr[@class='finished-comp']/td[2]")
	# comp_active = driver.find_elements_by_xpath("//tr[@class='active-comp']//div[@class='competition-details']/a ")
	# active_reward = driver.find_elements_by_xpath("//tr[@class='active-comp']/td[2]")
	image_finished = driver.find_elements_by_xpath("//tr[@class='finished-comp']/td/a/img")
	image_active = driver.find_elements_by_xpath("//tr[@class='active-comp']/td/a/img") 
	name_finished = driver.find_elements_by_xpath("//tr[@class='finished-comp']/td/a ")
	name_active = driver.find_elements_by_xpath("//tr[@class='active-comp']/td/a ")
	# for elem in range(len(comp_finisehd)):
	# 	df_comp.loc[elem] = [comp_finisehd[elem].text, finished_reward[elem].text, comp_finisehd[elem].get_attribute('href'),'finished']
	# for elem in range(len(comp_active)):
	# 	df_comp.loc[elem+len(comp_finisehd)] = [comp_active[elem].text, active_reward[elem].text, comp_active[elem].get_attribute('href'),'active']
	# print ('active number: ',len(comp_active),'finished number: ',len(comp_finisehd))
	for elem in range(len(image_finished)):
		urllib.urlretrieve(image_finished[elem].get_attribute('src'), 'images/'+name_finished[elem].get_attribute('href').split('/')[4]+'.png')
		print('done')
	for elem in range(len(image_active)):
		urllib.urlretrieve(image_active[elem].get_attribute('src'), 'images/'+name_active[elem].get_attribute('href').split('/')[4]+'.png')
		print('done')
	driver.quit()

	# for i in range(len(df_comp)):
	# 	df_comp_detailed.loc[i] = [df_comp.ix[i].Competition, df_comp.ix[i].Reward,df_comp.ix[i].URL, df_comp.ix[i].Status,
	# 								TextScraping(df_comp.ix[i].URL), df_comp.ix[i].URL + page[1], df_comp.ix[i].URL + page[2],
	# 								df_comp.ix[i].URL + page[3], df_comp.ix[i].URL + page[4], df_comp.ix[i].URL + page[5]]
	
	# df_comp_detailed.to_csv(output_file, encoding = 'utf-8')
	# print (df_comp_detailed)


def HtmlScraping():
	global page
	global file_names
	global df_comp_detailed
	workers = []
	#df_comp_detailed = DataFrame.from_csv('competitions_detailed.csv',encoding = 'utf-8')
	#driver = webdriver.Chrome()

	for i in range(len(df_comp_detailed)):
		MakeDir("Competitions/"+str(df_comp_detailed.ix[i].Competition))

	for ob in range(len(df_comp_detailed)):
		for i in range(6):
			worker=Thread(target=get_html,args=(df_comp_detailed.URL.ix[ob]+page[i],
						"Competitions/"+str(df_comp_detailed.ix[ob].Competition)+'/'+file_names[i],))
			
			worker.setDaemon(True)
			worker.start()
	#driver.quit()


if __name__ == "__main__":
	DataScraping()
	#HtmlScraping()



