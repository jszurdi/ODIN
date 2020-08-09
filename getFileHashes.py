import os
import sys
import csv
import hashlib
import psycopg2
import psycopg2.extras
import zlib
import sys

from database import RedirectDB


def concatenateLogFiles(downloadFolder,day):
  
  downloadFolder += '/'+day+'/'
  logfile = downloadFolder+'downloads.log'
  logout = open(logfile,mode = 'w')
  for path, subdirs, files in os.walk(downloadFolder):
    for name in files:
      filename = os.path.join(path, name)
      if(filename.endswith('log.tds.')):
        with open(filename) as fin:
          logout.write(fin.read())
  logout.close()
  return logfile
  

def getTargetId(db,url,day,experimentName):

  targetId = None
  with db.cursor() as cur:
    cur.execute("""SELECT target_id FROM targets 
                    WHERE day_added = '"""+day+"""' AND experiment_name = '"""+experimentName+"""' 
                    AND url = '"""+url+"""'""")
    targetId = cur.fetchone()[0]
  return targetId
  

def saveHashesToDatabase(db,targetId,filename,sha256Hash,md5Hash):

  query = ("""INSERT INTO downloads_hashes (target_id,filename,sha256,md5) 
              VALUES (%s, %s, %s, %s)""") 
  data = [targetId,filename,sha256Hash,md5Hash]
  with db.cursor() as cur:
    cur.execute(query, data)
  db.commit()
  
  
def saveHashes(logfile,db,day,experimentName):

  with open(logfile) as flog:
    reader = csv.reader(flog)
    for row in reader:
      filename,url = row
      with open(filename, mode='rb') as fin:
        fileContent = fin.read()
        sha256Hash = str(hashlib.sha256(fileContent).hexdigest())
        md5Hash = str(hashlib.md5(fileContent).hexdigest())
        targetId = getTargetId(db,url,day,experimentName)
        saveHashesToDatabase(db,targetId,filename,sha256Hash,md5Hash)
  
  
def getAndSaveHashesMain(runConfig):

  db = RedirectDB(runConfig)
  logfile = concatenateLogFiles(runConfig.downloadFolder,runConfig.day)
  saveHashes(logfile,db.db,runConfig.day,runConfig.experimentName)
  db.close()
  

if __name__ == '__main__':

  sys.path.append('../redirection_scanner')
  from config import Config
  
  runConfig = Config('../redirection_scanner/runConfig.txt')
  getAndSaveHashesMain(runConfig)
  
  