from urllib.request import urlopen
import urllib.parse
import json
import os
import time
import csv
import random

from database import RedirectDB

class GoogleSearch:


  def __init__(self,runConfig):
  
    # Search Engine
    self.engine = runConfig.searchEngine
    # Cybercrime group key
    self.key = runConfig.googleApiKey 
    self.keywordFolder = runConfig.searchKeywordFolder 
    self.dbname = runConfig.dbname
    self.runConfig = runConfig
    
    
  """
  This function takes in a list of words, and performs a google web search that
  attempts to be equivilent to searching for the words in the list seperated by spaces

  What is returned is the raw data returned from the API
  """
  def googleSearch(self,keywords):

    parameters = urllib.parse.quote(keywords[0])
    # Keyword concatenation
    for word in keywords[1:]:
      parameters = parameters + "+" + urllib.parse.quote(word)
    api_key = self.key[0]
    print("https://www.googleapis.com/customsearch/v1?key=" + api_key + "&cx=" + self.engine + "&q=" + parameters + "&num=10")
    for i in range(16):
      try:
        response = urlopen("https://www.googleapis.com/customsearch/v1?key=" + api_key + "&cx=" + self.engine + "&q=" + parameters + "&num=10")
      except Exception as e:
        print(e)
        print('Encountered error, retrying URL!')
        time.sleep(1)
      else:
        break
    data = response.read()
    encoding = response.info().get_content_charset('utf-8')
    return data.decode(encoding)


  """
  Program Start Here
  """
  def searchAll(self,type):

    db = RedirectDB(self.runConfig)
    requestDelay = 4 #Wait 4 seconds in between requests
    searchCounts = [] #set of lists, [topic, #keywords, #results, #errors]
    #Iterate over all files in the keywords folder
    file = self.keywordFolder + self.runConfig.searchKeywordFiles[0][type]
    searchResults = []
    with open(file) as fin:
      reader = csv.DictReader(fin)
      prevMeta = '-1'
      #Iterate over the keywords, space delimited
      for row in reader:
        keywordSets = eval(row['search_terms'])
        for keywords in keywordSets:
          search_term = row['subject'] + ' ' + keywords
          meta = type + ' - ' + row['type']
          if(meta != prevMeta):
            if(prevMeta != '-1'):
              searchCounts.append(counts)
            prevMeta = meta
            counts = [meta, 0, 0, 0]
          #Wait, dont want to request too fast
          time.sleep(requestDelay)
          counts[1] += 1 #Attempting the search of another set of keywords
          result = self.googleSearch(search_term.strip().split(" "))
          if result == "":
            #This is the error case
            counts[3] += 1
            continue
          data = json.loads(result)
          for i in range(len(data['items'])):
            searchResults.append([data, meta, type, i])
          #Add the number of results 
          counts[2] += len(data['items'])
      #Add a sample of the results to the database
      for r in random.sample(searchResults,min(len(searchResults),self.runConfig.pharmaSampleSize)):
        db.addSearch(r[0], r[1], r[2], r[3], self.runConfig)
      #Append the counts for this keyword file to the total counts list
      searchCounts.append(counts)
    db.close()
    return searchCounts
