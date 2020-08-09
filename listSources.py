import os
import csv
import time
import requests
import random
from subprocess import Popen
import gzip
import shutil

from database import RedirectDB
from moduleException import ModuleException
from typosquatting import Typosquatting
import copyright as cr

class ListSources:


  def __init__(self, runConfig):
  
    self.runConfig = runConfig
  
  
  def getUrlShortenerTargets(self):
    
    db = RedirectDB(self.runConfig)
    with open(self.runConfig.urlShortenerFile) as fin:
      csvreader = csv.reader(fin)
      for row in csvreader:
        url = row[0]
        if(row[0] != ''):
          db.addUrlsFromList(url,{'received':'20190221','src':'stonyBrook'},'stonyUrlShorteners',self.runConfig)
    db.close()
    
    
  def decodeSurbl(self, ipAddress):
  
    code = int(ipAddress.split('.')[-1])
    binaryStr = "{0:b}".format(code).zfill(8)
    lists = []
    if(binaryStr[0] == '1'):
      lists.append('cr')
    if(binaryStr[1] == '1'):
      lists.append('abuse')
    if(binaryStr[3] == '1'):
      lists.append('mw')
    if(binaryStr[4] == '1'):
      lists.append('ph')
    return lists


  def downloadSurbl(self, surblFile):
  
    if(not os.path.isfile(surblFile)):
      if(self.runConfig.day != time.strftime('%Y%m%d')):
        print('Error past surbl file not found: ',phishFile)
        raise ModuleException('Error past surbl file not found: '+phishFile)
      print('Downloading surbl file')
      os.environ["RSYNC_PROXY"] = "128.237.153.80:3128"
      command = ['rsync','-caz',self.runConfig.surblUrl,self.runConfig.surblLocation]
      proc = Popen(command)
      proc.wait()
      rawfilename = self.runConfig.surblLocation + 'surbl-raw.csv.gz'
      os.rename(rawfilename, surblFile)
      
    
  def loadSurbl(self):
  
    surblFile = self.runConfig.surblLocation+'surbl'+self.runConfig.day+'.csv.gz'
    if(not os.path.exists(self.runConfig.surblLocation)):
      os.makedirs(self.runConfig.surblLocation)
    self.downloadSurbl(surblFile)
    bls = {}
    unzippedFile = surblFile[:-3]
    with gzip.open(surblFile, 'rb') as f_in:
      with open(unzippedFile, 'wb') as f_out:
          shutil.copyfileobj(f_in, f_out)
    with open(unzippedFile) as fin:
      reader = csv.reader(fin)
      for row in reader:
        url = 'http://'+row[0]+'/'
        for ls in self.decodeSurbl(row[1]):
          if(ls not in bls):
            bls[ls] = set()
          bls[ls].add(url)
    os.remove(unzippedFile)
    selected = set()
    db = RedirectDB(self.runConfig)
    for ls in bls:
      samples = random.sample([x for x in bls[ls] if x not in selected],self.runConfig.surblSampleSize)
      selected = selected | set(samples)
      meta = {'type':ls,'weight':len(bls[ls])}
      print(meta)
      print(len(samples))
      for sample in samples:
        db.addUrlsFromList(sample,meta,'surbl-'+ls,self.runConfig)
    db.close()
    
    
  def downloadPhishTank(self,phishFile):
  
    if(not os.path.isfile(phishFile)):
      if(self.runConfig.day != time.strftime('%Y%m%d')):
        print('Error past phishTank file not found: ',phishFile)
        raise ModuleException('Error past phishTank file not found: '+phishFile)
      print('Downloading phishTank file')
      response = requests.get(self.runConfig.phishTankUrl)  
      with open(phishFile,mode='wb') as fout:
        fout.write(response.content)
    
    
  def loadPhishTank(self):
  
    db = RedirectDB(self.runConfig)
    phishFile = self.runConfig.phishTankLocation+'phishTank-'+self.runConfig.day+'.csv'
    self.downloadPhishTank(phishFile)
    with open(phishFile, encoding='utf-8') as fin:
      reader = csv.DictReader(fin)
      urls = {}
      for row in reader:
        url = row['url']
        meta = {'phish_detail_url':row['phish_detail_url'],
                'submission_time':row['submission_time'],
                'src':'phishTank'}
        urls[url] = meta
      samples = random.sample(list(urls.keys()),self.runConfig.phishTankSampleSize)
      for sample in samples:
        db.addUrlsFromList(sample,urls[sample],'phishTank',self.runConfig)
    db.close()
    
    
  def loadCopyright(self):
  
    db = RedirectDB(self.runConfig)
    movieTitles, seriesTitles = cr.getMovieTitles()
    finalUrls = cr.findIllegalStreams(movieTitles, seriesTitles)
    for url,meta in finalUrls.items():
      try:
        print(url)
      except:
        print('Url is not ascii')
      db.addUrlsFromList(url,meta,'copyright',self.runConfig)
    db.close()
    
    
  def testList(self):
  
    db = RedirectDB(self.runConfig)
    copyrightFile = self.runConfig.testListFile
    with open(copyrightFile) as fin:
      reader = csv.reader(fin)
      urls = {}
      for row in reader:
        url = row[0]
        meta = {'testName':'copyright test'}
        urls[url] = meta
      print('Test URLs loaded: ',len(urls))
      for url in urls:
        db.addUrlsFromList(url,urls[url],'listTest',self.runConfig)
    db.close()
    
    
  def getAlexaTargets(self):
  
    t = Typosquatting(self.runConfig)
    t.loadAlexaDomains(1,100000)
    sampleKeys = random.sample(list(t.alexaDomains.keys()),self.runConfig.alexaSampleSize)
    db = RedirectDB(self.runConfig)
    for key in sampleKeys:
      url = 'http://'+key+'/'
      meta = {'rank':t.alexaDomains[key]}
      db.addUrlsFromList(url,meta,'alexa',self.runConfig)
    db.close()

    
if __name__ == "__main__":
  
  from config import Config
  
  runConfig = Config('runConfig.txt')
  ls = ListSources(runConfig)
  # ls.getUrlShortenerTargets()
  # ls.loadSurbl()
  # ls.loadPhishTank()
  # ls.getAlexaTargets()
  ls.loadCopyright()