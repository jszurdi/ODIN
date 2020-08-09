
import os
import psycopg2
import psycopg2.extras
import zlib
import codecs
import pickle
import json
from pprint import pprint
from multiprocessing import Process
from multiprocessing import Queue
import multiprocessing
import time
import warnings
import sys

import redirectChainExtractor as rce
from database import RedirectDB
from config import Config



def saveRedirectionChains(targetId,scrapeId,windowId, crawlType, redirectionChain, contentDistribution, errorCodes, db, startUrl, isMainFrame):

  query = ("""INSERT INTO redirect_stats (target_id,scrape_id,window_id,crawl_type,redirect_chain,dom_redirect_chain,
                redirect_chain_len,dom_redirect_chain_len,content_distribution,dom_content_distribution,
                guessed_last_url,landing_domain,landing_dom_content_size,max_content_domain,
                max_content_dom_content_size,total_content_size,error_codes,is_main_frame) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")  
                
  # add click if it is a newly opened window and a new url is opened
  if(windowId is not None and redirectionChain[0].target != rce.getUrl(startUrl)):
    if(len(redirectionChain) > 1):
      del redirectionChain[0]
      redirectionChain.insert(0,rce.RedirectPair(rce.getUrl(startUrl),redirectionChain[0].src,'clickAction'))
      redirectionChain.insert(0,rce.RedirectPair(None,redirectionChain[0].src,'start'))
    else:
      redirectionChain = [rce.RedirectPair(None,rce.getUrl(startUrl),'start'),
                          rce.RedirectPair(rce.getUrl(startUrl),redirectionChain[0].target,'clickAction')]
    
  domRedirChain = rce.getDomainRedirectionChain(redirectionChain)
  domContentDistribution = rce.getPerDomainContent(contentDistribution)
  # make objects and lists json serializable
  redirectionChain = [{'src':str(rp.src),'target':str(rp.target),'type':rp.type} for rp in redirectionChain]
  domRedirChain = [{'src':rp.src,'target':rp.target,'type':rp.type} for rp in domRedirChain]
  
  if(len(domContentDistribution) > 0):
    maxContentDomain = max(domContentDistribution, key=domContentDistribution.get)
  else:
    maxContentDomain = 'error'
  contentDistribution = {str(k):i for k,i in contentDistribution.items()}
  errorCodes = {str(k):i for k,i in errorCodes.items()}

  redirLenModifier = 0
  if(domRedirChain[-1]['type'] == 'inclusion'):
    redirLenModifier = 1
  if(domRedirChain[-1]['target'] in domContentDistribution):
    landingDomContentSize = domContentDistribution[domRedirChain[-1]['target']]
  else:
    landingDomContentSize = -1
  if(maxContentDomain in domContentDistribution and maxContentDomain != 'error'):
    maxContentDomainContentSize = domContentDistribution[maxContentDomain]
  else:
    maxContentDomainContentSize = -1
  data = [targetId
          ,scrapeId
          ,windowId
          ,crawlType
          ,zlib.compress(json.dumps(redirectionChain).encode("utf-8"))
          ,zlib.compress(json.dumps(domRedirChain).encode("utf-8"))
          ,len(redirectionChain)-redirLenModifier
          ,len(domRedirChain)-redirLenModifier
          ,zlib.compress(json.dumps(contentDistribution).encode("utf-8"))
          ,zlib.compress(json.dumps(domContentDistribution).encode("utf-8"))
          ,None
          ,domRedirChain[-1]['target']
          ,landingDomContentSize
          ,maxContentDomain
          ,maxContentDomainContentSize
          ,sum(list(domContentDistribution.values()))
          ,zlib.compress(json.dumps(errorCodes).encode("utf-8"))
          ,isMainFrame]
  with db.cursor() as cur:
    cur.execute(query, data)


def getRedirectionsFromHar(runConfig,workQueue):

  db = RedirectDB(runConfig).db
  while True:
    job = workQueue.get()
    if(len(job) == 1 and job[0] == "DONE"):
      break

    targetId,scrapeId,har,perflog,crawlType,startUrl,landingUrl,afterClickhar,perWindowData,target_type = job
   
    if(har is None or len(har) == 0):
      print('Har None')
      return
    if(perflog is None or len(perflog) == 0):
      print('perflog None')
      return
    har = json.loads(har)
    performanceLog = json.loads(perflog)
    
    # main redirection chain
    try:
      redirectionChain, contentDistribution, errorCodes, notused = rce.getRedirectChain(performanceLog,har,startUrl,landingUrl)
      saveRedirectionChains(targetId,scrapeId,None, crawlType, redirectionChain, contentDistribution, errorCodes, db, startUrl,'yes')
    except:
      print('Main redirection extraction timed out: ',targetId, ' - ', scrapeId)
    
    # Ad redirect chain
    if(target_type == 'stonyUrlShorteners'):
      try:
        redirectionChains, contentDistribution, errorCodes, notused = rce.getAdRedirectChains(performanceLog,har,startUrl)
        for redirectionChain in redirectionChains:
          saveRedirectionChains(targetId,scrapeId,None, crawlType, redirectionChain, contentDistribution, errorCodes, db, startUrl,'no')
      except:
        print('Ad redirection extraction timed out: ',targetId, ' - ', scrapeId)
    
    # Secondary redirection chains after clicks
    if(afterClickhar is not None):
      afterClickhar = json.loads(afterClickhar)
      for id, data in perWindowData.items():
        try:
          redirectionChain, contentDistribution, errorCodes, notused = rce.getRedirectChain(json.loads(data['perfLog']),afterClickhar,data['startUrl'],data['landingUrl'])
          saveRedirectionChains(targetId,scrapeId,int(id), crawlType, redirectionChain, contentDistribution, errorCodes, db, startUrl,'yes')
        except:
          print('Window redirection extraction timed out: ',targetId, ' - ', scrapeId, ' - ', id)

  db.commit()
  db.close()

def multiProcessJobs(runConfig,jobs):

  numThreads = 64
  workQueue = Queue()
  threads = []
  for job in jobs:
    workQueue.put(job)
  
  for i in range(numThreads):
    p = Process(target = getRedirectionsFromHar, args = (runConfig,workQueue))
    p.start()
    threads.append(p)
  for i in range(numThreads):
    workQueue.put(["DONE"])
  print('Workers started', flush=True)
  for t in threads:
    t.join()
  
  
def decompressDict(input):
  
    return pickle.loads(zlib.decompress(input))
    
    
def addToPerWindowData(perWindowData,fieldName,field):

  if(field is None):
    return

  dataDict = decompressDict(field)
  for id, data in dataDict.items():
    if(id not in perWindowData):
      perWindowData[id] = {'startUrl':None,'landingUrl':None,'perfLog':None}
    perWindowData[id][fieldName] = data


def getRedirections(runConfig,all=False):
  
  db = RedirectDB(runConfig).db
  scrapeNames = ["'"+x['name']+"'" for x in runConfig.scrapeTypes]   
  with db.cursor(name='getRedirections '+str(os.getpid()),cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.itersize = 2000
    if(all):
      cur.execute("""SELECT t.target_id, t.url, s.scrape_id, s.name ,s.har, s.performance_log, s.after_click_har, 
                            s.after_click_urls, s.after_click_landing_urls, s.after_click_perflogs, s.landing_url, 
                            t.target_type 
                    FROM scrapes s 
                    LEFT OUTER JOIN redirect_stats r ON s.scrape_id = r.scrape_id 
                    JOIN targets t ON t.target_id = s.target_id 
                    AND r.redirect_stats_id IS NULL""")
    else:
      cur.execute("""SELECT t.target_id, t.url, s.scrape_id, s.name ,s.har, s.performance_log, s.after_click_har, 
                            s.after_click_urls, s.after_click_landing_urls, s.after_click_perflogs, s.landing_url, 
                            t.target_type 
                    FROM scrapes s 
                    LEFT OUTER JOIN redirect_stats r ON s.scrape_id = r.scrape_id 
                    JOIN targets t ON t.target_id = s.target_id 
                    WHERE t.day_added = '"""+runConfig.day+"""' 
                    AND t.experiment_name = '"""+runConfig.experimentName+"""' 
                    AND s.name IN ("""+', '.join(scrapeNames)+""") AND r.redirect_stats_id IS NULL""")
    i = 0
    start_time = time.time()
    jobs = []
    for row in cur:
      i += 1
      if(i % 400 == 0):
        print(i,' - ',row['target_id'])
        multiProcessJobs(runConfig,jobs)
        print("--- %s seconds ---" % (time.time() - start_time))
        jobs = []
      targetId = row['target_id']
      startUrl = row['url']
      if(row['har'] is not None and row['performance_log'] is not None):
        scrapeId = row['scrape_id']
        har = zlib.decompress(row['har']).decode("utf-8")
        perflog = zlib.decompress(row['performance_log']).decode("utf-8")
        landingUrl = row['landing_url']
        if(row['after_click_har'] is not None):
          afterClickhar = zlib.decompress(row['after_click_har']).decode("utf-8")
        else:
          afterClickhar = None
        perWindowData = {}
        addToPerWindowData(perWindowData,'startUrl',row['after_click_urls'])
        addToPerWindowData(perWindowData,'landingUrl',row['after_click_landing_urls'])
        addToPerWindowData(perWindowData,'perfLog',row['after_click_perflogs'])
        jobs.append([targetId,scrapeId,har,perflog,row['name'],startUrl,landingUrl,afterClickhar,perWindowData,row['target_type']])
    if(len(jobs) > 0):
      multiProcessJobs(runConfig,jobs)
        
  print("--- %s seconds ---" % (time.time() - start_time))
  db.close()

if __name__ == '__main__':

  runConfig = Config('runConfig.txt')
  warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
  getRedirections(runConfig,True)
  