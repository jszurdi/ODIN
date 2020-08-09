import csv
import time


class Config:


  def __init__(self,configFile):
  
    self.configFile = configFile
    self._loadConfig()

    
  def _loadConfig(self):
  
    with open(self.configFile, mode = 'r') as fin:
      csvReader = csv.DictReader(filter(lambda row: row[0]!='#' and row!='', fin))
      for row in csvReader:
        if(row['type'] == 'string'):
          setattr(self, row['key'], row['value'])
        elif(row['type'] == 'integer'):
          setattr(self, row['key'], int(row['value']))
        elif(row['type'] == 'float'):
          setattr(self, row['key'], float(row['value']))
        elif(row['type'] == 'boolean'):
          if(row['value'] in ['True','False']):
            setattr(self, row['key'], row['value'] == 'True')
          else:
            print('Boolean value is wrong for: ', row['key'])
            exit()
        elif(row['type'] == 'date'):
          setattr(self, row['key'], time.strftime(row['value']))
        elif(row['type'] == 'list'):
          setattr(self, row['key'], eval(row['value']))
        elif(row['type'] == 'json'):
          if(not hasattr(self, row['key'])):
            setattr(self, row['key'], [])
          getattr(self, row['key']).append(eval(row['value']))
        else:
          print('Type is wrong: ', row['type'])
          print(row)
          exit() 
          
          
if __name__ ==  '__main__':

  config = Config('runConfig.txt')
  # for key, item in config.__dict__.items():
    # print(key, ' - ', item)
  for scrapeType in config.scrapeTypes:
    if(scrapeType['ref']):
      print(scrapeType['name'],getattr(config,scrapeType['ua']))
    