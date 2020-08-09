from multiprocessing import Process
from multiprocessing import Queue
import multiprocessing
import subprocess
import time
import os
from pyvirtualdisplay import Display
import traceback
import logging
import random
import shutil

from linkFollow import LinkFollow
from database import RedirectDB
from proxy import Proxy


"""
Schedule the order in which target URLs should be visited to decrease the 
probability of being detected. Each job is an URL to be visited.
 - typo domains using the same name servers are visited as far apart as possible
 - different target types (like pharma and typo) are visited in an alternating fashion
"""
def scheduleJobs(db,jobs,day):

  print('Number of jobs: ',len(jobs))
  notTypoJobs = []
  typoJobs = []
  allJobs = []
  typoIds = {}
  # Separate different types of jobs.
  for job in jobs:
    if(job[2] in ['pharma','copyright','phishTank','stonyUrlShorteners','alexa',
                  'surbl-cr','surbl-abuse','surbl-mw','surbl-ph','listTest']):
      notTypoJobs.append(job)
    elif(job[2] in ['typosquatting','pharmaTypos','maliciousNsTypos',
                    'topAlexaTypos','midAlexaTypos','tailAlexaTypos']):
      typoIds[job[0]] = job
  random.seed(int(day)*len(jobs))
  random.shuffle(notTypoJobs)     
  # Get name servers for typo jobs.
  if(len(typoIds) > 0):
    nameServers = db.getNameservers(list(typoIds.keys()))
    typoJobsNotScheduled = True
    while(typoJobsNotScheduled):
      for ns in list(nameServers.keys()):
        typoId = nameServers[ns].pop()
        typoJobs.append(typoIds[typoId])
        if(len(nameServers[ns]) == 0):
          del nameServers[ns]
        if(len(nameServers) == 0):
          typoJobsNotScheduled = False       
  # Alternate jobs as evenly as possible   
  if(len(typoJobs) == 0 and len(notTypoJobs) == 0):
    pass
  elif(len(typoJobs) == 0):
    allJobs = notTypoJobs
  elif(len(notTypoJobs) == 0):
    allJobs = typoJobs
  else:
    ratio = max(1,int(len(typoJobs)/len(notTypoJobs))) + 1
    i = 0
    while(len(notTypoJobs) != 0 or len(typoJobs) != 0):
      i += 1
      if((i % ratio != 0) and (len(typoJobs) != 0)):
        allJobs.append(typoJobs.pop(0))
      elif(len(notTypoJobs) != 0):
        allJobs.append(notTypoJobs.pop(0))   
  return allJobs


class Crawler:


  def __init__(self,runConfig):
  
    self.runConfig = runConfig
    self.dbname = runConfig.dbname
    self.day = runConfig.day
    self.numThreads = runConfig.numThreads # 32
    self.numCrawlAttempts = runConfig.numCrawlAttempts #2
    self.batchSize = runConfig.batchSize # 600
    self.proxyPortBase = runConfig.proxyPortBase # 8200
    self.numScrapeRetriesCrawler = runConfig.numScrapeRetriesCrawler #2
    self.waitScrapeIntervals = runConfig.waitScrapeIntervals #20
    
    self.webProxies = self.loadProxies()
    random.shuffle(self.webProxies)
    self.proxyAssignment = self.getProxyAssignment()
    print('Proxy assignment: ',self.proxyAssignment)


  """
  Follows one link and returns the result
  """
  def followOneLink(self, job, linkFollower):

    doUserAction = False
    if(job[2] == 'copyright'):
      doUserAction = True

    for i in range(self.numScrapeRetriesCrawler):
      try:
        result = linkFollower.followLink(job[4], doUserAction=doUserAction)
        # This means linkFollower couldn't restart the browser and we should stop this thread.
        if(result is None):
          return 'linkFollower cannot start'
        # We got back some results
        if((result is not None) and (result[5] != "")):
          linkFollower.cleanup()
          return result
      # If something went wrong we can try again
      except Exception as e:
        logging.error(traceback.format_exc())
        print("Error Following Search Result: "+ str(job[4]) + " as "+linkFollower.name+" result")
        linkFollower.cleanup()
        continue
    # If we didn't return anything yet then return None
    return None

    
  """
  Stops drivers and proxies used for scraping
  """  
  def stopDriversAndProxies(self,linkFollowers,proxies):
  
    for linkFollower in linkFollowers:
      try:
        linkFollower.driver.quit()
      except:
        pass
    for proxy in proxies:
      try:
        proxy.close()
      except:
        pass
      
    
  """
  Starts drivers and proxies used for scraping
  """  
  def startMultipleLinkFollowersAndProxies(self,id,lock,webProxies,modifier):
  
    portBase = self.proxyPortBase+id  
    linkFollowers = []
    proxies = []
    lock.acquire()
    counter = 0
    for scrapeType in self.runConfig.scrapeTypes:
      if(len(webProxies) > 0 ):
        httpProxy = webProxies[(counter+modifier) % len(webProxies)]
      else:
        httpProxy = None
      proxy = Proxy(portBase+counter*200,httpProxy)
      if(not proxy.startProxy()):
        lock.release()
        return None, None
      proxies.append(proxy)
      linkFollowers.append(LinkFollow(id, self.runConfig, scrapeType, proxy, httpProxy))
      counter += 1
    lock.release()
    
    # If we were not able to start even one of the browser instances then quit this worker
    for linkFollower in linkFollowers:
      if(linkFollower is None):
        self.stopDriversAndProxies(linkFollowers,proxies)
        return None, None
    return linkFollowers, proxies
  
  
  """
  Stops one driver and proxy used for scraping
  """
  def stopOneDriverAndProxy(self,linkFollower,proxy):
  
    try:
      linkFollower.driver.quit()
    except:
      pass
    try:
      proxy.close()
    except:
      pass
    
    
  """
  Starts one driver and proxy used for scraping
  """
  def startOneDriverAndProxy(self,id,lock,scrapeType,webProxies,wpc):
  
    
    if(len(webProxies) > 0 ):
      httpProxy = webProxies[wpc % len(webProxies)]
    else:
      httpProxy = None
  
    portBase = self.proxyPortBase+id  
    lock.acquire()
    proxy = Proxy(portBase,httpProxy)
    if(not proxy.startProxy()):
      lock.release()
      return None, None
    lFollower = LinkFollow(id, self.runConfig, scrapeType, proxy, httpProxy)
    lock.release()
    return lFollower, proxy
    
    
  """
  Worker function

  This is a threaded function that pulls links out of a queue and then 
  performs 4 link follow requests using the default, referrer, android and googlebot 
  configurations
  """
  def worker(self, id, lock, workQueue, webProxies):

    print('worker starting: ', id)
    # Create the browser objects ahead of time and then re-use them for the current batch
    modCounter = 0
    modifier = len(self.runConfig.scrapeTypes)
    if(not self.runConfig.driverPerRequest):
      linkFollowers, proxies = self.startMultipleLinkFollowersAndProxies(id,lock,webProxies,modCounter*modifier)
      if(linkFollowers is None):
        return
    # If we have a new driver per each request then just put scrape type into linkFollowers
    else:
      linkFollowers = []
      for scrapeType in self.runConfig.scrapeTypes:
        linkFollowers.append(scrapeType)
    
    # Try to create a db connection
    try:
      db = RedirectDB(self.runConfig)
    except Exception as e:
      logging.error(traceback.format_exc())
      print("Error couldn't create db connection")
      return
      
    print('Ready for scraping: ',id)
    # Start scraping pages
    counter = 0
    while True:
      counter += 1
      # Get a page to scrape, if there are no more then stop!
      job = workQueue.get()
      if(len(job) == 1 and job[0] == "DONE"):
        break
      results = {}
      wpc = 0 # web proxy counter
      for linkFollower in linkFollowers:
        # Create proxy and driver then follow links then close proxy and driver
        if(self.runConfig.driverPerRequest):
          lFollower, proxy = self.startOneDriverAndProxy(id,lock,linkFollower,webProxies,wpc+counter*modifier)
          if(lFollower is None):
            continue
          result = self.followOneLink(job, lFollower)
          self.stopOneDriverAndProxy(lFollower,proxy)
          time.sleep(1)
        # Use proxies created earlier
        else:
          result = self.followOneLink(job, linkFollower)
        wpc += 1
        
        if(result == 'linkFollower cannot start'):
          return
        elif(result is not None):
          if(self.runConfig.driverPerRequest):
            results[linkFollower['name']] = {'result':result,'scrapeType':linkFollower}
          else:
            results[linkFollower.name] = {'result':result,'scrapeType':linkFollower.scrapeType}
          time.sleep(self.waitScrapeIntervals)
      # Update results
      if(len(results) > 0):
        db.addScrapes(job[0],results,self.runConfig)
      # Commit periodically
      if(counter % self.runConfig.commitFrequency == 0):
        db.commit()
      # If enough driver and proxy has been used for enough requests then restart them
      # This can only happen if driverPerRequest is False
      if(not self.runConfig.driverPerRequest and (counter % self.runConfig.requestPerDriver == 0)):
        modCounter += 1
        self.stopDriversAndProxies(linkFollowers,proxies)
        time.sleep(1)
        linkFollowers, proxies = self.startMultipleLinkFollowersAndProxies(id,lock,webProxies,modCounter*modifier)
        if(linkFollowers is None):
          return
          
    # Commit and close database connection
    db.commit()
    db.close()     
    # Attempt to stop browser instances and proxy when they are done.
    if(not self.runConfig.driverPerRequest):
      self.stopDriversAndProxies(linkFollowers,proxies)
      

  """
  The part responsible to run actual visits to webpages using multiprocessing.
  """
  def followLinksSample(self,links):
  
    workQueue = Queue()
    threads = []
    linksFollowed = 0
    for l in links:
      linksFollowed += 1
      workQueue.put(l)
    print("Finished adding the all links to the queue")
    #Create the virtual display adapter so that the link follower can work headless
    display = Display(visible = 0, size = (1280, 768))
    display.start()
    lock = multiprocessing.Lock()
    print("Finished creating virtual display adapter")
    # Start workers
    for i in range(self.numThreads):
      print('Process starting: ',i)
      p = Process(target = self.worker, args = (i, lock, workQueue, self.proxyAssignment[i]))
      p.start()
      threads.append(p)
    print('Workers started')
    #All the work has been added, tell the threads to quit now that things are done
    for i in range(self.numThreads):
      workQueue.put(["DONE"])

    #Join with the threads,
    for t in threads:
      t.join()
    # Stop processes
    display.stop() 
    # Cleanup if some processes didn't stop properly
    subprocess.call("pkill -f chrom", shell = True)
    subprocess.call("pkill -f mitm", shell = True)
    subprocess.call("pkill -f Xvfb", shell = True)
    time.sleep(8)
    
    return linksFollowed


  """
  Clean the /tmp folder to remove thrash left there by chromium
  """
  def cleanTmpFolder(self):
  
    for path, subdirs, files in os.walk('/tmp'):
      for subdir in subdirs:
        dirName = os.path.join(path, subdir)
        if(dirName.startswith('/tmp/.org.chromium')):
          try:
            shutil.rmtree(dirName)
          except:
            pass

  
  """
  Load list of web proxy urls to be used by the crawler
  """
  def loadProxies(self):
  
    if(self.runConfig.webProxyFile == ''):
      return []
      
    with open(self.runConfig.webProxyFile) as fin:
      return fin.read().split('\n')
      
  
  """
  Assign proxies to threads.
   - if there are less proxies than threads (including zero proxy)
     then all proxies are assigned to all threads
   - if there are more proxies than thread then they are distributed among threads
     as equally as possible
  """
  def getProxyAssignment(self):
    
    if(len(self.webProxies) < self.numThreads):
      return [self.webProxies] * self.numThreads
      
    assignments = [[] for i in range(self.numThreads)] 
    for j in range(len(self.webProxies)):
      i = j % self.numThreads
      assignments[i].append(self.webProxies[j])
    return assignments
      
  
  """
  The main function to run the crawler.
  """
  def followLinks(self):

    linksFollowed = 0
    # Try crawling all pages from where we don't have even one successful screenshot multiple times
    for i in range(self.numCrawlAttempts):
      db = RedirectDB(self.runConfig)
      cur = db.getUncrawledTargets(self.day,self.runConfig.experimentName,self.runConfig.scrapeTypes)
      links = scheduleJobs(db,list(cur),self.day)
      print('Jobs scheduled')
          
      # Execute jobs in smaller batches to solve the infrastructure going stale over time
      n = self.batchSize
      linksParts = [links[i:i+n] for i in range(0,len(links),n)]
      for linksPart in linksParts:
        linksFollowed += self.followLinksSample(linksPart)
            
      cur.close()
      db.close()
    self.cleanTmpFolder()
    return linksFollowed

