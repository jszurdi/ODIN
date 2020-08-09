import dhash
from PIL import Image
import distance
import csv
import os
import io
import psycopg2
import psycopg2.extras
import zlib
import pickle
import sys

from database import RedirectDB
     


def decompressDict(input):
  
    return pickle.loads(zlib.decompress(input))     


def loadHashes(days, experimentName):

  hashes = {}
  db = psycopg2.connect(dbname = 'tds')
  query = """SELECT p.hash, p.hash_id 
            FROM perceptual_hashes p 
            JOIN targets t ON t.target_id = p.target_id 
            WHERE t.day_added IN ("""+', '.join(["'"+x+"'" for x in days])+""") AND t.experiment_name = '"""+experimentName+"""'"""
  counter = 0
  with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.execute(query)
    for row in cur:
      counter += 1
      if(row['hash'] not in hashes):
        hashes[row['hash']] = []
      hashes[row['hash']].append(row['hash_id'])
  db.close()
  print('Number of rows processed: ', counter)
  print('Number of hashes loaded: ', len(hashes))
  return hashes
  
  
def topHashes(days,experimentName,dbname):

  minCount = '100'
  hashes = {}

  db = psycopg2.connect(dbname = dbname)
  query = """SELECT pc.hash, pc.cnt 
              FROM (SELECT p.hash, count(*) AS cnt 
              FROM perceptual_hashes p 
              JOIN targets t ON t.target_id = p.target_id 
              WHERE t.day_added IN ("""+', '.join(["'"+x+"'" for x in days])+""") 
              AND t.experiment_name = '"""+experimentName+"""' 
              GROUP BY p.hash) AS pc WHERE pc.cnt > """+minCount
  
  with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.execute(query)
    for row in cur:
      hashes[row['hash']] = row['hash']
      
  return {'topHashes':hashes}
    
    
def perceptualHashCluster(hashes,thresholds,outfile):

  clusters = {}
  invCluster = {}
  counter = 0
  for threshold in thresholds:
    if(threshold not in clusters):
      clusters[threshold] = {}
    if(threshold not in invCluster):
      invCluster[threshold] = {}
    for hash in hashes:
      counter += 1
      clusters[threshold][counter] = set([hash])
      invCluster[threshold][hash] = counter

  keys = list(hashes.keys())
  for i in range(len(keys)-1):
    if(i % 100 == 0):
      print(i,'/',len(keys),' hashes clustered')
    for j in range(i+1,len(keys)-1):
      # If they are in the same cluster already move on
      dist = distance.hamming(keys[i],keys[j])
      for threshold in thresholds:
        if((invCluster[threshold][keys[i]] != invCluster[threshold][keys[j]]) and (dist < threshold)):
          clust_i = invCluster[threshold][keys[i]]
          clust_j = invCluster[threshold][keys[j]]
          # update inverse pointers to clusters
          # and add members to new cluster
          for hash in clusters[threshold][clust_j]:
            invCluster[threshold][hash] = clust_i
            clusters[threshold][clust_i].add(hash)
          # remove other cluster
          del clusters[threshold][clust_j]
          
  for threshold in thresholds:
    print('Number of clusters: ',len(clusters[threshold]))
    with open(outfile+str(threshold)+'.csv',mode='w',newline='') as fout:
      fieldnames = ['clusterId','hash','hash_id']
      writer = csv.DictWriter(fout,fieldnames=fieldnames)
      for id, hashs in clusters[threshold].items():
        for hash in hashs:
          for hash_id in hashes[hash]:
            writer.writerow({'clusterId':id,'hash':hash,'hash_id':hash_id})
  
  return invCluster
  
  
def saveImages(invCluster,outFolder,days,experimentName,manyImages):

  db = psycopg2.connect(dbname = 'tds')
  for threshold in invCluster:
    folder = outFolder + str(threshold) + '/'
    if(not os.path.exists(folder)):
      os.makedirs(folder)
    query = """SELECT s.screenshot, p.hash, s.scrape_id, s.after_click_screenshots, p.window_id
            FROM scrapes s 
            JOIN perceptual_hashes p  ON s.scrape_id = p.scrape_id
            JOIN targets t ON t.target_id = s.target_id 
            WHERE t.day_added IN ("""+', '.join(["'"+x+"'" for x in days])+""") AND t.experiment_name = '"""+experimentName+"""'"""
    counter = 0
    with db.cursor(name='saveImages '+str(os.getpid()),cursor_factory=psycopg2.extras.DictCursor) as cur:
      cur.itersize = 200
      cur.execute(query)
      for row in cur:
        if(row['hash'] in invCluster[threshold]):
          if(manyImages):
            file = folder + str(invCluster[threshold][row['hash']]) + '-' + str(row['scrape_id']) + '-' + str(row['window_id']) + '.png'
          else:
            subfolder = folder + str(invCluster[threshold][row['hash']]) + '/'
            if(not os.path.exists(subfolder)):
              os.makedirs(subfolder)
            file = subfolder + str(row['scrape_id']) + '-' + str(row['window_id']) + '.png'
          found = False
          if(row['window_id'] is None):
            found = True
            with open(file, mode = 'wb') as fout:
              fout.write(zlib.decompress(row['screenshot']))    
          else:
            screenshots = decompressDict(row['after_click_screenshots'])
            for id, screenshot in screenshots.items():
              if(id == row['window_id']):
                found = True
                with open(file, mode = 'wb') as fout:
                  fout.write(screenshot) 
          if(not found):
            print('error, not found scrape_id: ', row['scrape_id'])
  db.close()
  

def getPngFiles(folder):

  return [folder+x for x in os.listdir(folder) if x.endswith('.png')]
  
  
def getPerceptualHashes(runConfig):
  
  db = RedirectDB(runConfig).db
  cout =  db.cursor()
  scrapeNames = ["'"+x['name']+"'" for x in runConfig.scrapeTypes] 
  with db.cursor(name='getPerceptualHashes '+str(os.getpid()),cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.itersize = 1000
    cur.execute("""SELECT s.scrape_id, s.target_id, s.screenshot, s.after_click_screenshots 
                    FROM scrapes s 
                    JOIN targets t ON t.target_id = s.target_id 
                    LEFT OUTER JOIN perceptual_hashes p ON s.scrape_id = p.scrape_id 
                    WHERE t.day_added = '"""+runConfig.day+"""' AND t.experiment_name = '"""+runConfig.experimentName+"""' 
                    AND s.name IN ("""+', '.join(scrapeNames)+""") AND p.hash_id IS NULL""")
    counter = 0
    for row in cur:
      counter += 1
      screenshot = zlib.decompress(row['screenshot'])
      image = Image.open(io.BytesIO(screenshot))
      r, c = dhash.dhash_row_col(image)
      hash = dhash.format_hex(r, c)
      query = """INSERT INTO perceptual_hashes (target_id, scrape_id, 
                  hash, maliciousness, window_id) 
                  VALUES (%s,%s,%s,%s,%s)"""
      data = [row['target_id'],
              row['scrape_id'],
              hash,
              'unknown',
              None]
      cout.execute(query,data)
      if(row['after_click_screenshots'] is not None):
        screenshots = decompressDict(row['after_click_screenshots'])
        for id, screenshot in screenshots.items():
          image = Image.open(io.BytesIO(screenshot))
          r, c = dhash.dhash_row_col(image)
          hash = dhash.format_hex(r, c)
          query = """INSERT INTO perceptual_hashes (target_id, scrape_id, 
                      hash, maliciousness, window_id) 
                      VALUES (%s,%s,%s,%s,%s)"""
          data = [row['target_id'],
                  row['scrape_id'],
                  hash,
                  'unknown',
                  id]
          cout.execute(query,data)
      if(counter % 100 == 0):
        print(counter)
  db.commit()
  cout.close()
  db.close()
  
  
def getPerceptualHashesAll():
  
  db = psycopg2.connect('dbname=tds')
  cout =  db.cursor()
  with db.cursor(name='getPerceptualHashes '+str(os.getpid()),cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.itersize = 1000
    cur.execute("""SELECT s.scrape_id, s.target_id, s.screenshot, s.after_click_screenshots 
                    FROM scrapes s 
                    JOIN targets t ON t.target_id = s.target_id 
                    LEFT OUTER JOIN perceptual_hashes p ON s.scrape_id = p.scrape_id 
                    WHERE p.hash_id IS NULL""")
    counter = 0
    for row in cur:
      counter += 1
      if(row['screenshot'] is not None):
        screenshot = zlib.decompress(row['screenshot'])
        image = Image.open(io.BytesIO(screenshot))
        r, c = dhash.dhash_row_col(image)
        hash = dhash.format_hex(r, c)
        query = """INSERT INTO perceptual_hashes (target_id, scrape_id, 
                    hash, maliciousness, window_id) 
                    VALUES (%s,%s,%s,%s,%s)"""
        data = [row['target_id'],
                row['scrape_id'],
                hash,
                'unknown',
                None]
        cout.execute(query,data)
      if(row['after_click_screenshots'] is not None):
        screenshots = decompressDict(row['after_click_screenshots'])
        for id, screenshot in screenshots.items():
          image = Image.open(io.BytesIO(screenshot))
          r, c = dhash.dhash_row_col(image)
          hash = dhash.format_hex(r, c)
          query = """INSERT INTO perceptual_hashes (target_id, scrape_id, 
                      hash, maliciousness, window_id) 
                      VALUES (%s,%s,%s,%s,%s)"""
          data = [row['target_id'],
                  row['scrape_id'],
                  hash,
                  'unknown',
                  id]
          cout.execute(query,data)
      if(counter % 100 == 0):
        print(counter)
  db.commit()
  cout.close()
  db.close()

  
def getMaliciousTargetIds(filein):

  maliciousIds = {}
  with open(filein) as fin:
    reader = csv.DictReader(fin)
    for row in reader:  
      row['targetId'] = int(row['targetId'])
      if(row['confidence'] == 'sure'):
        if(row['targetId'] not in maliciousIds):
          maliciousIds[row['targetId']] = set()
        maliciousIds[row['targetId']].add(row['crawlType'])
        
  print('Malciious target IDs: ',len(maliciousIds))
  return maliciousIds
  
  
def getMaliciousTargetIdsFromFolder(maliciousIds,folder):

  for path, subdirs, files in os.walk(folder):
    for name in files:
      targetId,crawlType = name[:-4].split('-')
      targetId = int(targetId)
      if(targetId not in maliciousIds):
        maliciousIds[targetId] = set()
      maliciousIds[targetId].add(crawlType)
  print('Malciious target IDs: ',len(maliciousIds))
  
  
def getMaliciousHashes(maliciousIds,db):
  
  hashes = set()
  with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.execute("""SELECT p.target_id, p.hash, s.name FROM perceptual_hashes p 
                JOIN scrapes s ON p.scrape_id = s.scrape_id 
                WHERE p.target_id IN ("""+','.join([str(x) for x in maliciousIds.keys()])+""")""")
    for row in cur:
      if(row['name'] in maliciousIds[row['target_id']]):
        hashes.add(row['hash'])
  print('Hashes loaded: ',len(hashes))
  return hashes
  
  
def setMaliciousnessForMatchings(maliciousHashes,db):

  with db.cursor() as cur:
    query = """UPDATE perceptual_hashes 
                SET maliciousness = 'malicious' 
                WHERE hash IN ("""+', '.join(["'"+x+"'" for x in maliciousHashes])+""")"""
    cur.execute(query)
  db.commit()
  
  
def setMaliciousness(maliciousHashes,dist,db,runConfig):

  cout = db.cursor()
  scrapeNames = ["'"+x['name']+"'" for x in runConfig.scrapeTypes] 
  with db.cursor(name='setMaliciousness '+str(os.getpid()),cursor_factory=psycopg2.extras.DictCursor) as cur:
    cur.itersize = 10000
    cur.execute("""SELECT hash_id, hash FROM perceptual_hashes p 
                  JOIN scrapes s ON s.scrape_id = p.scrape_id 
                  JOIN targets t ON t.target_id = p.target_id """)
                  
    malCounter = 0
    for row in cur:
      hash = row['hash']
      isMalicious = False
      for mHash in maliciousHashes:
        if(distance.hamming(hash,mHash) <= dist):
          isMalicious = True
          break
      if(isMalicious):
        malCounter += 1
        cout.execute("""UPDATE perceptual_hashes 
                   SET maliciousness = 'malicious' 
                   WHERE hash_id ="""+str(row['hash_id']))
  db.commit()
  cout.close()
  print('Malicious images found: ',malCounter)
  
  
def saveMaliciousHashes(hashes,filename):

  with open(filename, mode = 'w') as fout:
    fout.write('\n'.join(hashes))
    
    
def loadMaliciousHashes(filename):

  with open(filename) as fin:
    hashes = fin.read().split('\n')
  return hashes
  
  
def getMaliciousNameServers(db):

  outputFile = '../results/maliciousNameServers-20190220.csv'
  with db.cursor() as cur:
    query = """SELECT s2.name_server, MAX(s2.weight) as weight, 
        COUNT(s2.is_malicious) AS total, 
        SUM(s2.is_malicious) AS malicious_cnt, 
        AVG(s2.is_malicious) AS average  
        FROM (SELECT s1.name_server, MAX(s1.weight) as weight, s1.target_id, 
          CASE WHEN sum(s1.is_malicious) > 0 THEN 1 ELSE 0 END AS is_malicious  
          FROM (SELECT tt.name_server, tt.weight, t.target_id, 
            CASE WHEN p.maliciousness = 'malicious' THEN 1 ELSE 0 END AS is_malicious 
            FROM targets AS t 
            LEFT JOIN typo_targets AS tt ON t.target_id = tt.target_id 
            LEFT JOIN perceptual_hashes AS p ON t.target_id = p.target_id 
            WHERE tt.name_server IS NOT NULL AND tt.name_server != '') AS s1 
          GROUP BY (s1.name_server, s1.target_id)) AS s2 
        GROUP BY (name_server)
        HAVING AVG(s2.is_malicious) >= 0.01 
        ORDER BY average DESC, total DESC"""
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
    with open(outputFile, 'w') as f:
      cur.copy_expert(outputquery, f)
  

def getAndSetMaliciousness(runConfig):

  db = RedirectDB(runConfig).db

  maliciousIds = getMaliciousTargetIds('../results/simpleStats/20190127/manual-malicious-pages.csv')
  getMaliciousTargetIdsFromFolder(maliciousIds,'../samples/suspiciousScreenshots/')
  maliciousHashes  = getMaliciousHashes(maliciousIds, db)
  saveMaliciousHashes(maliciousHashes,'../results/simpleStats/20190127/maliciousHashes.txt')
  maliciousHashes = loadMaliciousHashes('../results/simpleStats/20190127/maliciousHashes.txt')
  setMaliciousness(maliciousHashes,4, db,runConfig)
  db.close()

    
if __name__ == "__main__":
  
  # Get missed perceptual hashes
  getPerceptualHashesAll()


