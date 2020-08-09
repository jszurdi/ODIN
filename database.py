import os
import time
import json
import psycopg2
from psycopg2 import IntegrityError
import psycopg2.extras
import traceback
import logging
import zlib
import pandas as pd
import pandas.io.sql as psql
import random
import pickle


class RedirectDB:


  def __init__(self, runConfig):
    
    self.dbname = runConfig.dbname
    if(runConfig.dbuser is not None and runConfig.dbuser != ''):
      print('Remote connection')
      self.db = psycopg2.connect(dbname = runConfig.dbname, 
                                  user = runConfig.dbuser, 
                                  password = runConfig.dbpwd,
                                  host = runConfig.dbhost,
                                  port = runConfig.dbport)
    else:
      self.db = psycopg2.connect(dbname = runConfig.dbname)
    
    
  def close(self):
  
    self.db.close()


  def getDatabase(self):
  
    return self.db
    
    
  def commit(self):
  
    return self.db.commit()
    
    
  def getDatabaseSize(self):
    
    size = 0
    with self.db.cursor() as cur:
      cur.execute("""SELECT pg_database_size('"""+self.dbname+"""')""")
      size = cur.fetchone()[0] 
    return size


  def getUncrawledTargets(self, day, experimentName, scrapeTypes):
  
    c = self.db.cursor()
    scrapeNames = ["'"+x['name']+"'" for x in scrapeTypes]              
    query = """SELECT t2.target_id, t2.time_added, t2.target_type, t2.day_added, t2.url 
              FROM (SELECT t1.target_id, t1.time_added, t1.target_type, t1.day_added, t1.url, sum(t1.isScraped) as cntScraped 
              FROM (SELECT  t.target_id, t.time_added, t.target_type, t.day_added, t.url, 
              CASE WHEN  s.name IN ("""+', '.join(scrapeNames)+""") THEN 1 ELSE 0 END AS isScraped 
              FROM targets t 
              LEFT OUTER JOIN scrapes s ON t.target_id = s.target_id 
              WHERE t.day_added = '"""+day+"""' AND t.experiment_name = '"""+experimentName+"""') AS t1 
              GROUP BY t1.target_id, t1.time_added, t1.target_type, t1.day_added, t1.url) as t2 
              WHERE cntScraped = 0"""
    c.execute(query)
    return c
    
    
  def copyTargets(self, runConfig):
    
    cout = self.db.cursor()
    with self.db.cursor(name='copyTargets '+str(os.getpid()),cursor_factory=psycopg2.extras.DictCursor) as cur:
      cur.itersize = 1000
      query = """SELECT *
                FROM targets t 
                LEFT OUTER JOIN typo_targets tt on t.target_id = tt.target_id 
                LEFT OUTER JOIN pharma_targets o on t.target_id = o.target_id 
                LEFT OUTER JOIN list_targets l on t.target_id = l.target_id 
                WHERE t.day_added = '"""+runConfig.repeatDay+"""' AND t.experiment_name = '"""+runConfig.repeatExperimentName+"""'"""
      cur.execute(query)
      for row in cur:
        # Insert into targets
        query_target = ("""INSERT INTO targets (time_added, target_type, day_added, url, 
                        experiment_name, day_repeated) 
                        VALUES (%s, %s, %s, %s, %s, %s) 
                        ON CONFLICT ON CONSTRAINT targets_day_added_url_target_type_experiment_name_key DO NOTHING 
                        RETURNING target_id""")                    
        data_target = [str(time.time())
                      ,row['target_type']
                      ,runConfig.day
                      ,row['url']
                      ,runConfig.experimentName
                      ,runConfig.repeatDay]
        cout.execute(query_target, data_target)
        if(cout.rowcount == 1):
          target_id = cout.fetchone()[0] 
          # insert into typo targets
          if(row['typo_id'] is not None):
            query_typo = ("""INSERT INTO typo_targets (target_id, typo_domain, original_domain, 
                            alexa_rank, mistake_type, name_server, weight) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)""")
            data_typo = [target_id
                          ,row['typo_domain']
                          ,row['original_domain']
                          ,row['alexa_rank']
                          ,row['mistake_type']
                          ,row['name_server']
                          ,row['weight']]
            cout.execute(query_typo, data_typo) 
          # insert into pharma targets
          elif(row['pharma_id'] is not None):
            query_pharma = ("""INSERT INTO pharma_targets (target_id, keywords, raw_data, total_results, 
                            title, link, display_link, snippet, rank, meta_tag) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
            data_pharma = [target_id
                          ,row['keywords']
                          ,row['raw_data']
                          ,row['total_results']
                          ,row['title']
                          ,row['link']
                          ,row['display_link']
                          ,row['snippet']
                          ,row['rank']
                          ,row['meta_tag']]
            cout.execute(query_pharma, data_pharma) 
          # insert into list targets
          elif(row['list_id'] is not None):
            query_list = ("""INSERT INTO list_targets (target_id, meta_data) VALUES (%s, %s)""")
            data_list = [target_id, row['meta_data']]
            cout.execute(query_list, data_list) 
    cout.close()
    self.db.commit()
    
    
  def getNameservers(self,typoIds):
    
    nameServers = {}
    with self.db.cursor(cursor_factory=psycopg2.extras.DictCursor) as c:
      query = """SELECT t.target_id as target_id, tt.name_server as name_server
                FROM targets t 
                JOIN typo_targets tt ON t.target_id = tt.target_id 
                WHERE t.target_id IN ("""+",".join([str(x) for x in typoIds])+""")"""
      c.execute(query)
      for row in c:
        nameServer = row['name_server'].split(';')[0]
        if(nameServer not in nameServers):
          nameServers[nameServer] = []
        nameServers[nameServer].append(row['target_id'])
      
    for ns in nameServers:
      random.shuffle(nameServers[ns])
      
    return nameServers


  def addTyposquatting(self, targets, type, runConfig):
  
    with self.db.cursor() as cur:
      for domain, row in targets.items():
        # Inserting into target and making sure we don't add duplicate urls on the same day
        query_target = ("""INSERT INTO targets (time_added, target_type, day_added, url, 
                      experiment_name, day_repeated) 
                      VALUES (%s, %s, %s, %s, %s, %s) 
                      ON CONFLICT ON CONSTRAINT targets_day_added_url_target_type_experiment_name_key DO NOTHING 
                      RETURNING target_id""")                    
        data_target = [str(time.time())
                      ,type
                      ,runConfig.day
                      ,"http://"+domain+"/"
                      ,runConfig.experimentName
                      ,runConfig.repeatDay]
        cur.execute(query_target, data_target)
        
        # Inserting into pharma target
        if(cur.rowcount == 1):
          target_id = cur.fetchone()[0] 
          query_typo = ("""INSERT INTO typo_targets (target_id, typo_domain, original_domain, 
                          alexa_rank, mistake_type, name_server, weight) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s)""")
          data_typo = [target_id
                        ,domain
                        ,row["alexaDomain"]
                        ,row["rank"]
                        ,row["operation"]
                        ,row["nameServer"]
                        ,row["weight"]]
          cur.execute(query_typo, data_typo) 
    self.db.commit()

    
  """
  Take in the raw output of a google custom search api result and parse out the fields and 
  make new database entries corresponding to it
  """
  def addSearch(self, raw_search, meta_tag, type, rank, runConfig):
  
    cur = self.db.cursor()
    print("Number of results: " + str(len(raw_search['items'])))
    
    # Inserting into target and making sure we don't add duplicate urls on the same day
    query_target = ("""INSERT INTO targets (time_added, target_type, day_added, url, 
                  experiment_name, day_repeated) 
                  VALUES (%s, %s, %s, %s, %s, %s) 
                  ON CONFLICT ON CONSTRAINT targets_day_added_url_target_type_experiment_name_key DO NOTHING 
                  RETURNING target_id""")                    
    data_target = [str(time.time())
                  ,type
                  ,runConfig.day
                  ,raw_search['items'][rank]['link']
                  ,runConfig.experimentName
                  ,runConfig.repeatDay]
    cur.execute(query_target, data_target)
    # Inserting into pharma target
    if(cur.rowcount == 1):
      target_id = cur.fetchone()[0] 
      query_pharma = ("""INSERT INTO pharma_targets (target_id, keywords, raw_data, total_results, 
                      title, link, display_link, snippet, rank, meta_tag) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
      if('nextPage' in raw_search['queries']):
        totalResults = raw_search['queries']['nextPage'][0]['totalResults']
      else:
        totalResults = "-1"
      data_pharma = [target_id
                    ,str(raw_search['queries']['request'][0]['searchTerms'])
                    ,zlib.compress(json.dumps(raw_search).encode("utf-8"))
                    ,int(totalResults)
                    ,raw_search['items'][rank]['title']
                    ,raw_search['items'][rank]['link']
                    ,raw_search['items'][rank]['displayLink']
                    ,raw_search['items'][rank]['snippet']
                    ,rank
                    ,meta_tag]
      cur.execute(query_pharma, data_pharma) 
    cur.close()
    self.db.commit()

    
  """
  Add urls from a static list
  """
  def addUrlsFromList(self, url, meta, type, runConfig):
  
    cur = self.db.cursor()
    # Inserting into target and making sure we don't add duplicate urls on the same day
    query_target = ("""INSERT INTO targets (time_added, target_type, day_added, url, 
                  experiment_name, day_repeated) 
                  VALUES (%s, %s, %s, %s, %s, %s) 
                  ON CONFLICT ON CONSTRAINT targets_day_added_url_target_type_experiment_name_key DO NOTHING 
                  RETURNING target_id""")                    
    data_target = [str(time.time())
                  ,type
                  ,runConfig.day
                  ,url
                  ,runConfig.experimentName
                  ,runConfig.repeatDay]
    cur.execute(query_target, data_target)
    # Inserting into list target
    if(cur.rowcount == 1):
      target_id = cur.fetchone()[0] 
      query_list = ("""INSERT INTO list_targets (target_id, meta_data) 
                      VALUES (%s, %s)""")
      data_list = [target_id
                    ,zlib.compress(json.dumps(meta).encode("utf-8"))]
      cur.execute(query_list, data_list) 
    cur.close()
    self.db.commit()
    
    
  def addRunConfig(self, runConfig):
  
    omitFields = set(['watchdogEmail','watchdogEmailPassword','server','recipientEmail',
                      'searchEngine','googleApiKey','searchKeywordFolder','dbname','createTargets'
                      'proxyPortBase','extensionsFolder'])
    configToSave = json.dumps({x:y for x,y in runConfig.__dict__.items() if x not in omitFields})
    query = """INSERT INTO run_config (run_config) VALUES (%s) RETURNING config_id"""
    config_id = None
    with self.db.cursor() as cur:
      cur.execute(query, [zlib.compress(configToSave.encode("utf-8"))])
      config_id = cur.fetchone()[0] 
    return config_id
    
    
  def compressDict(self, dictObject):
  
    return zlib.compress(pickle.dumps(dictObject))
      
    
  def addScrapes(self, target_id, results, runConfig):
  
    for name,item in results.items():
      result = item['result']
      scrapeType = item['scrapeType']
      data = [None] * 21
      data[0] = target_id
      data[1] = runConfig.configId
      data[2] = scrapeType['name']
      data[3] = runConfig.location
      data[4] = scrapeType['ua']
      if(scrapeType['ref']):
        data[5] = runConfig.referer
      else:
        data[5] = ''
      data[6] = scrapeType['browser']
      data[7] = scrapeType['mobile']
      data[8] = result[0] # time
      data[9] = zlib.compress(result[4].encode("utf-8")) # har
      data[10] = zlib.compress(result[6].encode("utf-8")) # perflog
      data[11] = zlib.compress(result[3].encode("utf-8")) # html
      data[12] = zlib.compress(result[5]) # screenshot
      data[19] = result[2] # landing url
      data[20] = result[8]# proxy
      if(result[7] is not None and result[7]['perWindowData'] is not None):
        perWindowData = result[7]['perWindowData']
        data[13] = self.compressDict({k:i[2] for k,i in perWindowData.items()})
        data[14] = zlib.compress(result[7]['har'].encode("utf-8"))
        data[15] = self.compressDict({k:i[1] for k,i in perWindowData.items()})
        data[16] = self.compressDict({k:i[0] for k,i in perWindowData.items()})
        data[17] = self.compressDict({k:i[3] for k,i in perWindowData.items()})
        data[18] = self.compressDict({k:i[4] for k,i in perWindowData.items()})
      
      query = ("""INSERT INTO scrapes (target_id, config_id, name, scrape_location, 
                      useragent, referrer, browser_type, mobile_emulation, scrape_time,
                      har, performance_log, html, screenshot, after_click_urls, 
                      after_click_har, after_click_htmls, after_click_screenshots, 
                      after_click_landing_urls, after_click_perflogs,landing_url, http_proxy)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
      
      with self.db.cursor() as cur:
        try:
          cur.execute(query, data)
        except Exception as e:
          print('Saving to db failed: ', e)
          print('Target id: ',target_id)
      
    
  def getDailyScrapeStats(self,day):
  
    query = """SELECT 
                  SUM(s1.har_referrer_success) AS har_referrer_success,
                  SUM(s1.har_googlebot_success) AS har_googlebot_success,
                  SUM(s1.har_vanilla_success) AS har_vanilla_success,
                  SUM(s1.har_android_success) AS har_android_success,
                  SUM(s1.missed) AS missed,
                  s1.target_type AS target_type
                FROM (SELECT 
                  CASE WHEN s.har IS NOT NULL AND s.name = 'referrer' THEN 1 ELSE 0 END AS har_referrer_success,
                  CASE WHEN s.har IS NOT NULL AND s.name = 'googlebot' THEN 1 ELSE 0 END AS har_googlebot_success,
                  CASE WHEN s.har IS NOT NULL AND s.name = 'vanilla' THEN 1 ELSE 0 END AS har_vanilla_success,
                  CASE WHEN s.har IS NOT NULL AND s.name = 'android' THEN 1 ELSE 0 END AS har_android_success,
                  CASE WHEN s.har IS NULL THEN 1 ELSE 0 END AS missed,
                  t.target_type AS target_type
                FROM targets t 
                LEFT JOIN scrapes s ON t.target_id = s.target_id 
                WHERE t.day_added = '"""+day+"""') AS s1 
                GROUP BY s1.target_type;"""
    pd.set_option('expand_frame_repr', False)
    return str(psql.read_sql(query, self.db))


if __name__ == "__main__":

  db = RedirectDB("tds")
  dailyStat = db.getDailyScrapeStats("20190208")
  print(dailyStat)
  print('----\n')
  dailyStat = db.getDailyScrapeStats("20190210")
  print(dailyStat)
  print('----\n')
  dailyStat = db.getDailyScrapeStats("20190211")
  print(dailyStat)
  db.close()
