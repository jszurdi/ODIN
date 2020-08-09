import time
import smtplib
import os
import math
import datetime
from multiprocessing import Process
import warnings
import sys
import csv
import psycopg2.extras
import logging

from search import GoogleSearch
from crawler import Crawler, scheduleJobs
from database import RedirectDB
from typosquatting import Typosquatting
from listSources import ListSources
from config import Config

from getRedirections import getRedirections
from getFileHashes import getAndSaveHashesMain
from imageClustering import getPerceptualHashes, getAndSetMaliciousness

    
class Odin:


  def __init__(self,runConfig):
  
    self.runConfig = runConfig
    self.db = RedirectDB(self.runConfig)
    self.startTime = None
    self.databaseSize = None
    self.searchTime = None
    self.searchSize = None
    self.crawlTime = None
    self.crawlSize = None
    self.searchCounts = []
    
  def __del__(self):
  
    try:
      self.db.db.close()
    except:
      pass
    
  def sendEmail(self,msg):
    
    server = smtplib.SMTP(self.runConfig.server)
    server.ehlo()
    server.starttls()
    server.login(self.runConfig.watchdogEmail, self.runConfig.watchdogEmailPassword)
    server.sendmail(self.runConfig.watchdogEmail, self.runConfig.recipientEmail, msg)
    server.quit()
    
    
  def getRunStatistics(self,name,startTime,endTime,startData,endData):
  
    timeDiff = endTime - startTime
    totalHours = math.floor((timeDiff)/(60*60))
    timeDiff -= 60*60*totalHours
    totalMinutes = math.floor((timeDiff)/60)
    timeDiff -= 60*totalMinutes
    totalSeconds = timeDiff
    stats = name+" Statistics\n\nDuration: [" + str(startTime) + "-" + str(endTime) + "] " + str(totalHours) + " hours, " + str(totalMinutes) + " minutes, " + str(totalSeconds) + " seconds\n"
    stats += "Data Added: [" + str(startData) + "-" + str(endData) + "] " + str((endData - startData)/(1024*1024)) + "MB\n"
    return stats
    

  def getTypoTargets(self):
  
    if('typosquatting' in self.runConfig.createTargets or 
      'pharmaTypos' in self.runConfig.createTargets or
      'maliciousNsTypos' in self.runConfig.createTargets or
      'tailTypos' in self.runConfig.createTargets):
      print("Loading alexa domains")
      t = Typosquatting(self.runConfig)
      t.loadAlexaDomains(1,1000000)
    if('typosquatting' in self.runConfig.createTargets):
      print("Creating typosquatting targets")
      t.getTyposquattingTargets(500)
    if('pharmaTypos' in self.runConfig.createTargets):
      print("Creating pharma typosquatting targets")
      t.getPharmaTyposquattingTargets(10000000)
    if('tailTypos' in self.runConfig.createTargets):
      t.getComAlexaDomains()
      t.getLongTaileTypos()
    if('maliciousNsTypos' in self.runConfig.createTargets):
      t.getMaliciousTyposquattingTargets(500)
    
    
  def getOtherTargets(self):
  
    # Get targets from various lists
    if('phishTank' in self.runConfig.createTargets or
      'stonyUrlShorteners' in self.runConfig.createTargets or
      'alexa' in self.runConfig.createTargets or
      'surbl' in self.runConfig.createTargets or
      'copyright' in self.runConfig.createTargets or
      'listTest' in self.runConfig.createTargets):
      ls = ListSources(self.runConfig)
      if('stonyUrlShorteners' in self.runConfig.createTargets):
        ls.getUrlShortenerTargets()
      if('phishTank' in self.runConfig.createTargets):
        ls.loadPhishTank()
      if('alexa' in self.runConfig.createTargets):
        ls.getAlexaTargets()
      if('surbl' in self.runConfig.createTargets):
        ls.loadSurbl()
      if('copyright' in self.runConfig.createTargets):
        ls.loadCopyright()
      if('listTest' in self.runConfig.createTargets):
        ls.testList()
    # Get Google Search targets
    if('pharma' in self.runConfig.createTargets):
      print("Performing Searches")
      googleSearch = GoogleSearch(self.runConfig)
      if('pharma' in self.runConfig.createTargets):
        self.searchCounts.extend(googleSearch.searchAll('pharma'))
      print("Finished Searches")
    
    
  def exportTargetsToCsv(self):
  
    # Get link orders
    outputFile = self.runConfig.urlsDirectory + 'targetUrls-'+self.runConfig.day+'.csv'
    c = self.db.getUncrawledTargets(self.runConfig.day,self.runConfig.experimentName,self.runConfig.scrapeTypes)
    links = scheduleJobs(self.db,list(c),self.runConfig.day)
    ids = [x[0] for x in links]
  
    # Get links
    targets = {}
    with self.db.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
      query = """SELECT target_id, time_added, target_type, day_added, experiment_name, url, day_repeated 
                FROM targets WHERE day_added = '"""+self.runConfig.day+"""'""" 
      cur.execute(query)
      for row in cur:
        targets[row['target_id']] = row
      
    # Save links in order
    with open(outputFile, 'w',encoding='utf-8-sig',newline='') as fout:
      headers = 'target_id, time_added, target_type, day_added, experiment_name, url, day_repeated'.split(', ')
      writer = csv.DictWriter(fout,fieldnames=headers)
      writer.writeheader()
      for id in ids:
        writer.writerow(targets[id])
        
    
  def getTargets(self):

    self.startTime = time.time()
    self.databaseSize = self.db.getDatabaseSize()

    if(self.runConfig.repeatDay != '' and self.runConfig.repeatExperimentName != ''):
      self.db.copyTargets(self.runConfig)
      self.searchTime = time.time()
      self.searchSize = self.db.getDatabaseSize()
      return
    elif(self.runConfig.repeatDay != '' or self.runConfig.repeatExperimentName != ''):
      print('Repeat day error')
      self.searchTime = time.time()
      self.searchSize = self.db.getDatabaseSize()
      return
    
    pTypo = Process(target = self.getTypoTargets)
    pTypo.start()
    #Do a search of all the seach terms
    pSearch = Process(target = self.getOtherTargets)
    pSearch.start()
    # join target creation processes when they finished
    pTypo.join()
    pSearch.join()
      
    self.searchTime = time.time()
    self.searchSize = self.db.getDatabaseSize()
    
    # Save targets as a csv file
    if(self.runConfig.urlsDirectory != ''):
      if(os.path.isdir(self.runConfig.urlsDirectory)):
        self.exportTargetsToCsv()
      else:
        print("Couldn't save target urls to directory because it doesn't exists: ",self.runConfig.urlsDirectory)
        exit()

      
  def followLinks(self):
  
    print("Following Links")
    configId = self.db.addRunConfig(self.runConfig)
    if(configId is None):
      print('Error setting configId')
      exit()
    self.db.commit()
    setattr(self.runConfig, 'configId', configId)
    crawlerInst = Crawler(self.runConfig)
    linksFollowed = crawlerInst.followLinks()
    self.crawlTime = time.time()
    self.crawlSize = self.db.getDatabaseSize()
    
    # Generate the final email message
    print("Generating and Sending Emails")
    totalStats = self.getRunStatistics('Batch',self.startTime,self.crawlTime,self.databaseSize,self.crawlSize)
    searchStats = self.getRunStatistics('Batch',self.startTime,self.searchTime,self.databaseSize,self.searchSize)
    crawlStats = self.getRunStatistics('Batch',self.searchTime,self.crawlTime,self.searchSize,self.crawlSize)
    if(self.runConfig.createTargets):
      for s in self.searchCounts:
        searchStats += s[0] + ": " + str(s[1]) + " keyword searches, " + str(s[2]) + " results, " + str(s[3]) + " errors\n"

    message = "Subject: Odin Update\n\n" + totalStats + "\n\n\n" + searchStats + "\n\n\n" + crawlStats
    message += "\n\n\nScrape success:\n\n"
    message += self.db.getDailyScrapeStats(self.runConfig.day)

    #Send an email with the statistics
    self.sendEmail(message)
    
    
  def runProcessing(self):

    warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
    logging.basicConfig(level=logging.ERROR)
    
    print('Extracting redirections')
    getRedirections(self.runConfig)
    print('Extracting download hashes')
    getAndSaveHashesMain(self.runConfig)
    print('Perceptual hashes')
    getPerceptualHashes(self.runConfig)
    print('Set maliciousness based on known bad perceptual hashes')
    getAndSetMaliciousness(self.runConfig)
    print('Done')
    
  def main(self):
  
    self.getTargets()
    if(self.runConfig.followLinks):
      self.followLinks()
      self.runProcessing()
    
if __name__ ==  '__main__':

  runConfig = Config('runConfig.txt')
  odin = Odin(runConfig)
  odin.main()
  
