import requests
from zipfile import ZipFile
import tldextract
from collections import OrderedDict
import random
from bs4 import BeautifulSoup
from ftplib import FTP
import hashlib
from functools import partial
import gzip
import shutil
import subprocess
import json
import os
import time
from multiprocessing import Process
from multiprocessing import Manager
import csv

from batchDNS import resolveDns
from database import RedirectDB
from moduleException import ModuleException

class Typosquatting:


  def __init__(self,runConfig):
  
    self.dbname = runConfig.dbname
    self.day = runConfig.day
    self.dataFolder = runConfig.typoFolder
    self.runConfig = runConfig
    self.alexaLocation = "http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
    if(not os.path.exists(self.dataFolder)):
      os.makedirs(self.dataFolder)
    
    self.alexaDomains = {}
    self.comAlexaDomains = {}
    self.alexaPharmaDomains = {}
    self.allTypoTargets = set()
    self.typoDomains = {}
    self.pharmaTypoDomains = {}
    self.longTailTypos = {}
    
    self.validDomainChars = ['a','b','c','d','e','f','g','h','i','j'
                            ,'k','l','m','n','o','p','q','r','s','t'
                            ,'u','v','w','x','y','z','0','1','2','3'
                            ,'4','5','6','7','8','9','-']
    self.defensiveNameServers = ['markmonitor.com','googledomains.com','citizenhawk.net ']
    self.alexaPharmaUrls = ["https://www.alexa.com/topsites/category/Top/Health/Pharmacy",
                            "https://www.alexa.com/topsites/category/Top/Health/Men's_Health"]
    self.dnsIntensity = self.runConfig.dnsIntensity
    
  
  def downloadAlexa(self,alexaFile):
  
    if(not os.path.isfile(alexaFile)):
      # We can only download alexa files today and not for past days
      if(self.day != time.strftime('%Y%m%d')):
        print('Error past alexa file not found: ',alexaFile)
        raise ModuleException('Error past alexa file not found: '+alexaFile)
      print('Downloading alexa file')
      response = requests.get(self.alexaLocation)  
      with open(alexaFile,mode='wb') as fout:
        fout.write(response.content)
    
    
  def loadAlexaDomains(self,minRank,maxRank):
  
    folder = self.dataFolder+'alexa/'
    if(not os.path.exists(folder)):
      os.makedirs(folder)
    alexaFile = folder+'alexa-'+self.day+'.csv.zip'
    # If alexa file does not exists then download it
    self.downloadAlexa(alexaFile)
       
   # Unzip and load alexa file
    zipfile = ZipFile(alexaFile)
    for line in zipfile.open("top-1m.csv").readlines():
        rank, domain = line.decode('utf-8')[:-1].split(',')
        rank = int(rank)
        if(rank >= minRank and rank <= maxRank):
          domain = tldextract.extract(domain.lower()).registered_domain
          registeredPart = domain.split('.')[0]
          if(domain not in self.alexaDomains and len(registeredPart) > 3):
            self.alexaDomains[domain] = rank
    print('Number of alexa domains loaded: ',len(self.alexaDomains))
    
    
  def getComAlexaDomains(self):
  
    for domain, rank in self.alexaDomains.items():
      if(domain.endswith('.com') and len(domain[:-4]) > 3):
        if(domain[:-4] not in self.comAlexaDomains or 
            self.comAlexaDomains[domain[:-4]] > rank):
          self.comAlexaDomains[domain[:-4]] = rank
    print('Number of .com alexa domains loaded: ',len(self.comAlexaDomains))
    
    
  def loadAlexaPharmaDomains(self):
  
    for url in self.alexaPharmaUrls:
      page = requests.get(url)
      soup = BeautifulSoup(page.text, 'html.parser')
      containers = soup.findAll('div', {'class': 'DescriptionCell'})
      for container in containers:
        domain = container.find('a').get_text()
        domain =  tldextract.extract(domain.lower()).registered_domain
        registeredPart = domain.split('.')[0]
        if(domain not in self.alexaPharmaDomains and len(registeredPart) > 3):
          if(domain in self.alexaDomains):
            self.alexaPharmaDomains[domain] = self.alexaDomains[domain]
          else:
            self.alexaPharmaDomains[domain] = None
    print('Number of alexa pharma domains loaded: ',len(self.alexaPharmaDomains))
    

  def changeCharacter(self,alexaDomainList, char, i):
      typoDomainList = list(alexaDomainList)
      if(typoDomainList[i] == char):
          return None
      else:
          typoDomainList[i] = char
          return ''.join(typoDomainList)
          
          
  def deleteCharacter(self,alexaDomainList,i):
      typoDomainList = list(alexaDomainList)
      typoDomainList.pop(i)
      return ''.join(typoDomainList)
      
      
  def addCharacter(self,alexaDomainList,char,i):
      typoDomainList = list(alexaDomainList)
      typoDomainList.insert(i,char)
      return ''.join(typoDomainList)
      
      
  def changeUpTwoCharacters(self,alexaDomainList,i):
      typoDomainList = list(alexaDomainList)
      if(typoDomainList[i] == typoDomainList[i+1]):
          return None
      else:
          char = typoDomainList.pop(i)
          typoDomainList.insert(i+1,char)
          return ''.join(typoDomainList)
  
  
  def addOneTypoDomain(self,typoDomain,typoDomains,alexaDomain,suffix,operation,alexaDomains):
 
    if(typoDomain is not None and len(typoDomain) > 2 and 
      typoDomain[0] != '-' and typoDomain[-1] != '-'):
      fullTypoDomain = typoDomain + suffix
    else:
      return
      
    if(fullTypoDomain not in typoDomains):
      typoDomains[fullTypoDomain] = {'alexaDomain':alexaDomain
                                ,'operation':operation
                                ,'rank':alexaDomains[alexaDomain]}

  
  def getTypoDomains(self,alexaDomain,alexaDomains,comOnly=False):
  
    typoDomains = {}
    prefix = alexaDomain.split('.')[0]
    if(comOnly):
      suffix = ''
    else:
      suffix = '.'+'.'.join(alexaDomain.split('.')[1:])
    alexaDomainList = list(prefix)
    
    # We iterate on the characters of the alexaDomain to calculate new typo domains
    max = len(alexaDomainList)
    for i in range(max):
        # 2 Replace each character
        for char in self.validDomainChars:
            typoDomain = self.changeCharacter(alexaDomainList,char,i)
            self.addOneTypoDomain(typoDomain,typoDomains,alexaDomain,suffix,'mis',alexaDomains)
        # 2 Delete character
        typoDomain = self.deleteCharacter(alexaDomainList,i)
        self.addOneTypoDomain(typoDomain,typoDomains,alexaDomain,suffix,'del',alexaDomains)
        # 3 Add character
        for char in self.validDomainChars:
            typoDomain = self.addCharacter(alexaDomainList,char,i)
            self.addOneTypoDomain(typoDomain,typoDomains,alexaDomain,suffix,'add',alexaDomains)
        #If two char are the same after each other no new typo is generated
        # 4 Change up two characters
        if (i!=(max-1)):
            typoDomain = self.changeUpTwoCharacters(alexaDomainList,i)
            self.addOneTypoDomain(typoDomain,typoDomains,alexaDomain,suffix,'chng',alexaDomains)
    # 3.b Add character to the last place
    for char in self.validDomainChars:
        typoDomain = self.addCharacter(alexaDomainList,char,max)
        self.addOneTypoDomain(typoDomain,typoDomains,alexaDomain,suffix,'add',alexaDomains)
    # 5 Append www at the beginning
    self.addOneTypoDomain('www'+prefix,typoDomains,alexaDomain,suffix,'www',alexaDomains)
      
    return typoDomains

    
  def generateTypoVariantsInMemory(self,minRank,targetDomains,typoDict):
  
    for targetDomain in targetDomains:
      rank = targetDomains[targetDomain]
      if(rank is not None and rank <= minRank):
        typoDomains = self.getTypoDomains(targetDomain,self.alexaDomains)
        for typoDomain in typoDomains:
          if((typoDomain not in typoDict) or
            (typoDomains[typoDomain]['rank'] is not None and 
            typoDomains[typoDomain]['rank'] < typoDict[typoDomain]['rank'])):
            typoDict[typoDomain] = typoDomains[typoDomain]
    print('Number of typo domains generated: ',len(typoDict))
    
          
  def getDnsLessDomains(self,typoDict, recordTypes):
  
    rTypes = set(recordTypes)
    domains = [dom for dom in typoDict 
                if len(rTypes & set(typoDict[dom].keys())) != len(rTypes)]
    print('Number of domains without a DNS record: ',len(domains))
    return domains
   
              
  def loadDnsRecords(self,typoDict,recordTypes):
    
    domains = self.getDnsLessDomains(typoDict,[x.lower()+'s' for x in recordTypes])
    responseDict = resolveDns(domains,recordTypes,self.dnsIntensity)
    for domain, records in responseDict.items():
      for type, results in records.items(): 
        if(type not in typoDict[domain]):
          typoDict[domain][type] = set()
        for result in results:
          typoDict[domain][type].add(result)
          
    
  def getRegisteredNss(self,nss):
  
    registeredNss = set()
    for ns in nss:
      domain = tldextract.extract(ns).registered_domain.lower()
      if(domain == ''):
        registeredNss.add(ns.lower())
      else:
        registeredNss.add(domain)
    return registeredNss
      
    
  def selectTargets(self,typoDict,alexaNss):
    
    sampleSizeTop = int(self.runConfig.typoSampleSize/2)
    sampleSizeBottom = int(self.runConfig.typoSampleSize/2)
    noTopNss = 40
    topNsSampleSize = int(sampleSizeTop/noTopNss)
    
    nameServers = {}
    cTypos = [dom for dom in typoDict 
                if 'nss' in typoDict[dom] and 'as' in typoDict[dom]
                and dom not in self.allTypoTargets
                and 'nss' in alexaNss[typoDict[dom]['alexaDomain']]]
    print('cTypos************: ',len(cTypos))
    for cTypo in cTypos:
      alexaDomain = typoDict[cTypo]['alexaDomain']
      registeredNss = self.getRegisteredNss(typoDict[cTypo]['nss'])
      nsInter = registeredNss & self.getRegisteredNss(alexaNss[alexaDomain]['nss'])
      if(alexaDomain in self.alexaDomains):
        rank = self.alexaDomains[alexaDomain]
      else:
        rank = 4444444444444444
      if(cTypo in self.alexaDomains):
        typoRank = self.alexaDomains[cTypo]
      else:
        typoRank = 8888888888888888
      for nsRegistered in registeredNss:
        if(nsRegistered != alexaDomain and 
          nsRegistered not in self.defensiveNameServers and
          rank < typoRank and
          len(nsInter) == 0):
          if(nsRegistered not in nameServers):
            nameServers[nsRegistered] = set()
          nameServers[nsRegistered].add(cTypo)
    print('nameServers************: ',len(nameServers))
    topNameServers = OrderedDict(sorted(nameServers.items(), key=lambda x: len(x[1]), reverse = True))
    
    targets = {}
    # Adding domains at popular name servers
    for nameServer in list(topNameServers.keys())[:noTopNss]:
      selDomains = random.sample(list(topNameServers[nameServer]),
                                  min(topNsSampleSize*4,len(list(topNameServers[nameServer]))))
      selDomains = [x for x in selDomains if x not in targets]
      for domain in selDomains[:topNsSampleSize]:
        targets[domain] = typoDict[domain]
        targets[domain]['nameServer'] = nameServer
        targets[domain]['weight'] = len(topNameServers[nameServer])
        self.allTypoTargets.add(domain)
    print('targets************: ',len(targets))
    # Adding domains at less popular name servers
    allDomains = {} 
    for nameServer in list(topNameServers.keys())[noTopNss:]:
      for domain in topNameServers[nameServer]:
        if(domain not in allDomains):
          allDomains[domain] = nameServer
    allDomains = {x:y for x,y in allDomains.items() if x not in targets}
    print('allDomains************: ',len(allDomains))
    for domain in random.sample(list(allDomains.keys()),min(sampleSizeBottom,len(list(allDomains.keys())))):
      targets[domain] = typoDict[domain]
      targets[domain]['nameServer'] = allDomains[domain]
      targets[domain]['weight'] = len(topNameServers[allDomains[domain]])
      self.allTypoTargets.add(domain)
    print('targets************: ',len(targets))
      
    print("Targets selected: ", len(targets))
    return targets
    
    
  def getAlexaNss(self,alexaDomains):
    
    alexaNss = {dom:{} for dom in alexaDomains}
    self.loadDnsRecords(alexaNss,("NS",))
    self.loadDnsRecords(alexaNss,("NS",))
    return alexaNss
      
 
  def getTyposquattingTargets(self, maxRank):

    self.generateTypoVariantsInMemory(maxRank,self.alexaDomains,self.typoDomains)
    self.loadDnsRecords(self.typoDomains,("NS","A"))
    self.loadDnsRecords(self.typoDomains,("NS","A"))
    self.getDnsLessDomains(self.typoDomains,("nss","as"))
    alexaNss = self.getAlexaNss(self.alexaDomains)
    targets = self.selectTargets(self.typoDomains,alexaNss)
    db = RedirectDB(self.runConfig)
    db.addTyposquatting(targets, "typosquatting", self.runConfig)
    db.close()
    
    
  def getPharmaTyposquattingTargets(self, maxRank):  

    self.loadAlexaPharmaDomains()
    self.generateTypoVariantsInMemory(maxRank,self.alexaPharmaDomains,self.pharmaTypoDomains)
    self.loadDnsRecords(self.pharmaTypoDomains,("NS","A"))
    self.loadDnsRecords(self.pharmaTypoDomains,("NS","A"))
    self.getDnsLessDomains(self.pharmaTypoDomains,("nss","as"))
    alexaNss = self.getAlexaNss(self.alexaPharmaDomains)
    pharmaTargets = self.selectTargets(self.pharmaTypoDomains,alexaNss)
    db = RedirectDB(self.runConfig)
    db.addTyposquatting(pharmaTargets, "pharmaTypos", self.runConfig)
    db.close()
 
 
  def loadMaliciousNameServers(self):
  
    nss = set()
    fin = open(self.runConfig.maliciousNsFile)
    reader = csv.DictReader(fin)
    for row in reader:
      nss.update(row['name_server'].split(';'))
    fin.close()
    return nss

    
  def selectMaliciousTargets(self,typoDict,alexaNss):
    
    sampleSize = self.runConfig.maliciousPerNsSampleSize
    nsMaliciousSet = self.loadMaliciousNameServers()
    nameServers = {}
    cTypos = [dom for dom in typoDict 
                if 'nss' in typoDict[dom] and 'as' in typoDict[dom]
                and dom not in self.allTypoTargets
                and 'nss' in alexaNss[typoDict[dom]['alexaDomain']]]
    for cTypo in cTypos:
      alexaDomain = typoDict[cTypo]['alexaDomain']
      registeredNss = self.getRegisteredNss(typoDict[cTypo]['nss'])
      nsInter = registeredNss & self.getRegisteredNss(alexaNss[alexaDomain]['nss'])
      if(alexaDomain in self.alexaDomains):
        rank = self.alexaDomains[alexaDomain]
      else:
        rank = 4444444444444444
      if(cTypo in self.alexaDomains):
        typoRank = self.alexaDomains[cTypo]
      else:
        typoRank = 8888888888888888
      for nsRegistered in registeredNss:
        if(nsRegistered != alexaDomain and 
          nsRegistered not in self.defensiveNameServers and
          nsRegistered in nsMaliciousSet and
          rank < typoRank and
          len(nsInter) == 0):
          if(nsRegistered not in nameServers):
            nameServers[nsRegistered] = set()
          nameServers[nsRegistered].add(cTypo)
    
    targets = {}
    # Adding domains at popular name servers
    for nameServer in nameServers:
      selDomains = random.sample(list(nameServers[nameServer]),
                                  min([sampleSize*2,len(list(nameServers[nameServer]))]))
      selDomains = [x for x in selDomains if x not in targets]
      for domain in selDomains[:sampleSize]:
        targets[domain] = typoDict[domain]
        targets[domain]['nameServer'] = nameServer
        targets[domain]['weight'] = len(nameServers[nameServer])
        self.allTypoTargets.add(domain)
      
    print("Targets selected: ", len(targets))
    return targets
    
 
  def getMaliciousTyposquattingTargets(self, maxRank):

    self.generateTypoVariantsInMemory(maxRank,self.alexaDomains,self.typoDomains)
    self.loadDnsRecords(self.typoDomains,("NS","A"))
    self.loadDnsRecords(self.typoDomains,("NS","A"))
    self.getDnsLessDomains(self.typoDomains,("nss","as"))
    alexaNss = self.getAlexaNss(self.alexaDomains)
    targets = self.selectMaliciousTargets(self.typoDomains,alexaNss)
    db = RedirectDB(self.runConfig)
    db.addTyposquatting(targets, "maliciousNsTypos", self.runConfig)
    db.close()
  
  
  def md5sum(self,filename):
  
    with open(filename, mode='rb') as f:
      d = hashlib.md5()
      for buf in iter(partial(f.read, 128), b''):
        d.update(buf)
    return d.hexdigest()
  
  
  def downloadComZone(self):
  
    if(not os.path.isfile(self.comZone)):
      # We can only download zone files today and not for past days
      if(self.day != time.strftime('%Y%m%d')):
        print('Error past zone file not found: ',self.comZone)
        raise ModuleException('Error past zone file not found: '+self.comZone)
  
      ftp = FTP('rz.verisign-grs.com')
      ftp.login('jszurdi','mellonUn1!')
      with open(self.comZone, 'wb') as localfile:
        ftp.retrbinary('RETR ' + 'com.zone.gz', localfile.write)
      with open(self.comZone+'.md5', 'wb') as localfile:
        ftp.retrbinary('RETR ' + 'com.zone.gz.md5', localfile.write)
      ftp.quit()
      
      md5 = self.md5sum(self.comZone)
      with open(self.comZone+'.md5') as fin:
        md5Test = fin.read()
      if(str(md5) == str(md5Test)):
        print('Md5 hash matches')
        return True
      else:
        print('Error md5 hash does not match')
        print(md5)
        print(md5Test)
        return False
      
      
  """
  This function converts a sorted zone file to CSV.
  This compresses the number of files used too.
  Parameters:
    inputfile - the name of the zone file
  	  outputfile - the name of the zone file in new form
  """
  def generateCsvFileFromZone(self, inputfile, outputfile):

    fopen = open(inputfile, mode='r')
    fwrite = open(outputfile, mode='w')
    nsList = []
    prevDomain = ''
    for line in fopen:
      fields = line.split(' ')
      if(len(fields) > 1 and fields[1] == 'NS'):    #If the line contains NS info
        if(fields[0] == prevDomain):    #If the prevDomain is equal with the line domain, then append to list
          nsList.append(fields[2][:-1])
        else:   #If the prevDomain is not equal with the line domain, then write to file
          if(prevDomain != ''):   #First the prevDomain is empty, we don't want to write anything
            fwrite.write(prevDomain + ' ' + ' '.join(nsList) + '\n')
          nsList = [fields[2][:-1]]
          prevDomain = fields[0]
    fopen.close()
    fwrite.close()
  
  
  def transformZone(self):
    
    with gzip.open(self.comZone, 'rb') as fin:
      with open(self.comZone[:-3], 'wb') as fout:
        shutil.copyfileobj(fin, fout)
    
    subprocess.call("LC_COLLATE=C sort -o "+self.comZone[:-3]+"-sorted "+self.comZone[:-3], shell = True)
    os.remove(self.comZone[:-3])
    self.generateCsvFileFromZone(self.comZone[:-3]+"-sorted",self.comZone[:-3]+".csv")
    os.remove(self.comZone[:-3]+"-sorted")
    
    
  def downloadAndTransformZone(self):
    
    self.downloadComZone()
    self.transformZone()
    
    
  def collapseGeneratedTypos(self):
  
    fin = open('gtypos.json-sorted', mode = 'r')
    fout = open('gtypos-collapsed.json', mode = 'w')
    noGtypos = 0
    noCollapsedGtypos = 0
    prevTypoDomain = -1
    typoOriginals = {}
    for line in fin:
      noGtypos += 1
      typoDomain = line[:-1].split(' ')[0]
      typoData = json.loads(' '.join(line[:-1].split(' ')[1:]))
      if((prevTypoDomain != typoDomain) and (prevTypoDomain != -1)):
        noCollapsedGtypos +=1
        fout.write(prevTypoDomain + ' ' + json.dumps(typoOriginals) + '\n')
        typoOriginals = {}
        
      if(typoData['alexaDomain'] not in typoOriginals):
        typoOriginals[typoData['alexaDomain']] = {'rank':typoData['rank'],
                                                  'operation':typoData['operation']}
      else:
        rank_prev = typoOriginals[typoData['alexaDomain']]['rank']
        rank = typoData['rank']
        if(rank > rank_prev):
          typoOriginals[typoData['alexaDomain']] = {'rank':typoData['rank'],
                                                  'operation':typoData['operation']}
      prevTypoDomain = typoDomain
        
    print('noGtypos' + str(noGtypos))    
    print('noCollapsedGtypos' + str(noCollapsedGtypos))     
    fout.close()
    fin.close()
    
    
  def generateTypoVariants(self, alexaNss):
  
    with open('gtypos.json', mode='w') as fout:
      for alexaDomain in self.comAlexaDomains:
        if('nss' in alexaNss[alexaDomain+'.com']):
          typoDomains = self.getTypoDomains(alexaDomain,self.comAlexaDomains,comOnly=True)
          for typoDomain,items in typoDomains.items():
            fout.write(typoDomain + ' ' + json.dumps(items) + '\n')
    subprocess.call("LC_COLLATE=C sort -o gtypos.json-sorted gtypos.json", shell = True)
    os.remove("gtypos.json")
    self.collapseGeneratedTypos()
    os.remove("gtypos.json-sorted")
   
   
  def addOriginalToTypo(self, typoDomain, alexaDomain, alexaData, typoDomains,registeredNss,alexaNss):
  
    if(alexaDomain+'.com' not in alexaNss or 'nss' not in alexaNss[alexaDomain+'.com']):
      return 1
    alexaInter = registeredNss & self.getRegisteredNss(alexaNss[alexaDomain+'.com']['nss'])
    defensiveInter = registeredNss & set(self.defensiveNameServers)
    alexaDomain not in registeredNss
    if(len(alexaInter) == 0 and
        len(defensiveInter) == 0 and
        alexaDomain not in registeredNss and
        typoDomain not in self.allTypoTargets):
      if(typoDomain not in typoDomains):
        typoDomains[typoDomain] = {'typoDomain':typoDomain,'originalDomains':{},'nameServers':[]}
        for ns in registeredNss:
          typoDomains[typoDomain]['nameServers'].append(ns)
      if(alexaDomain not in typoDomains[typoDomain]['originalDomains']):
        typoDomains[typoDomain]['originalDomains'][alexaDomain] = alexaData
      else:
        print("Error: shouldn't be more than one of each alexaDomains for a typoDomain")
      return 0
    else:
      return 1
  
   
  def addTypoDomain(self,typoDomain,typoData,zoneAttrs,typoDomains,alexaDomains,alexaNss):
  
    zoneAttrs = [ns+'.com' if ns[-1] != '.' else ns[:-1] for ns in zoneAttrs]
    registeredNss = self.getRegisteredNss(zoneAttrs)
  
    leftOutDomains = 0
    if(typoDomain in alexaDomains):
      typoRank = alexaDomains[typoDomain]
      for alexaDomain in typoData:
        if(alexaDomain in alexaDomains):
          alexaRank = alexaDomains[alexaDomain]
          if(int(alexaRank) < int(typoRank)):
            leftOutDomains += self.addOriginalToTypo(typoDomain,alexaDomain,typoData[alexaDomain],typoDomains,registeredNss,alexaNss)
          else:
            leftOutDomains += 1
        else:
          leftOutDomains += 1
    else:
      for alexaDomain in typoData:
        leftOutDomains += self.addOriginalToTypo(typoDomain,alexaDomain,typoData[alexaDomain],typoDomains,registeredNss,alexaNss)
    return leftOutDomains
    
  
  def saveCandidateTypos(self):
  
    folder = self.dataFolder+'ctypos/'
    if(not os.path.exists(folder)):
      os.makedirs(folder)
    with open(folder+self.day+'-ctypos.json', mode='w') as fout:
      for key,item in self.longTailTypos.items():
        fout.write(json.dumps(item)+'\n')
   
   
  def loadCandidateTypos(self):
  
    with open(self.dataFolder+'ctypos/'+self.day+'-ctypos.json') as fin:
      for line in fin:
        item = json.loads(line[:-1])
        self.longTailTypos[item['typoDomain']] = item
   
    
  def getCandidateTypos(self,alexaNss):
  
    if(os.path.isfile(self.dataFolder+'ctypos/'+self.day+'-ctypos.json')):
      self.loadCandidateTypos()
    else:
      zonef = open(self.comZone[:-3]+".csv", mode='r')  
      typof = open('gtypos-collapsed.json', mode='r')
      typoline = typof.readline()                        
      typoDomain = typoline[:-1].split(' ')[0]
      zoneline = zonef.readline()
      zoneDomain = zoneline[:-1].split(' ')[0]   
      leftOutCtypos = 0 
      while (typoline != "" and zoneline != ""): 
        if (typoDomain.lower() == zoneDomain.lower()):     
          typoData = json.loads(' '.join(typoline[:-1].split(' ')[1:]))
          zoneAttrs = zoneline[:-1].lower().split(' ')[1:]
          leftOutCtypos += self.addTypoDomain(typoDomain,typoData,zoneAttrs,self.longTailTypos,self.comAlexaDomains,alexaNss)
          #Check for double presence of typo domain
          oldTypoDomain = typoDomain.lower()
          typoline = typof.readline()                   
          typoDomain = typoline[:-1].split(' ')[0]
          if(oldTypoDomain != typoDomain.lower()):
            zoneline = zonef.readline()
            zoneDomain = zoneline[:-1].split(' ')[0]
        elif (typoDomain.lower() > zoneDomain.lower()): 
          zoneline = zonef.readline()
          zoneDomain = zoneline[:-1].split(' ')[0]
        else:     
          typoline = typof.readline()
          typoDomain = typoline[:-1].split(' ')[0]
      typof.close()       
      zonef.close()  
               
      os.remove(self.comZone[:-3]+".csv")
      os.remove('gtypos-collapsed.json')
      self.saveCandidateTypos()
      print("ctypo - original pairs left out: " + str(leftOutCtypos) + '\n')    
    print(": Total ctypos loaded " + str(len(self.longTailTypos)) + '\n')   
    
    
  def getTyposByPopularity(self,minRank,maxRank):
  
    cTypos = {}
    for typo,item in self.longTailTypos.items():
      typo += '.com'
      # Get smallest ranked original domain as the most likely typosquatting target
      rank = None
      operation = None
      originalDomain = None
      for oDomain, alexaData in item['originalDomains'].items():
        if(rank is None or alexaData['rank'] < rank):
          rank = alexaData['rank']
          operation = alexaData['operation']
          originalDomain = oDomain
      # if ctypo's target is in the alexa range add to ctypos
      if(rank >= minRank and rank <= maxRank):
        cTypos[typo] = {}
        cTypos[typo]['alexaDomain'] = originalDomain+'.com'
        cTypos[typo]['rank'] = rank
        cTypos[typo]['operation'] = operation
        nss = [x[:-1] if x.endswith('.') else x+'.com' for x in item['nameServers']]
        cTypos[typo]['nameServer'] = ';'.join(nss)
        cTypos[typo]['weight'] = -1
    return cTypos
   

  def getTypoSample(self,minRank,maxRank,sampleSize):
  
    typoDomains = self.getTyposByPopularity(minRank,maxRank)
    sample = {x:typoDomains[x] for x in random.sample(list(typoDomains),sampleSize)}
    # Make sure selected typo targets are not selected again
    self.allTypoTargets |= set(sample.keys())
    return sample
   

  def getLongTaileTypos(self):
  
    alexaNss = self.getAlexaNss([d+'.com' for d in self.comAlexaDomains])
    folder = self.dataFolder+'com-zone/'
    if(not os.path.exists(folder)):
      os.makedirs(folder)
    self.comZone = folder+self.day+'-com.zone.gz'
    # Getting long tail typosquatting domains
    p1 = Process(target = self.downloadAndTransformZone)
    p1.start()
    p2 = Process(target = self.generateTypoVariants, args = (alexaNss, ))
    p2.start()
    p1.join()
    p2.join()
    self.getCandidateTypos(alexaNss)
    # Get top, mid an tail typo samples
    topTypoDomainsSample = self.getTypoSample(1,10000,self.runConfig.topTypoSampleSize)
    midTypoDomainsSample = self.getTypoSample(10001,250000,self.runConfig.midTypoSampleSize)
    tailTypoDomainsSample = self.getTypoSample(250001,1000000,self.runConfig.tailTypoSampleSize)
    # Save results to database
    db = RedirectDB(self.runConfig)
    db.addTyposquatting(topTypoDomainsSample, "topAlexaTypos", self.runConfig)
    db.addTyposquatting(midTypoDomainsSample, "midAlexaTypos", self.runConfig)
    db.addTyposquatting(tailTypoDomainsSample, "tailAlexaTypos", self.runConfig)
    db.close()
    
  
if __name__ ==  '__main__':
  
  from config import Config

  runConfig = Config('runConfig.txt')
  t = Typosquatting(runConfig)
  t.loadAlexaDomains(1,1000000)
  # t.getComAlexaDomains()
  # # Getting top typosquatting domains
  # t.getTyposquattingTargets(500)
  # t.getPharmaTyposquattingTargets(10000000)
  # t.getMaliciousTyposquattingTargets(500)
  # t.getLongTaileTypos()
  
  
 
  