from subprocess import Popen, PIPE
from urllib import request as urlrequest
from urllib.parse import urlencode, quote_plus
import pickle
import time
import json

class Proxy:


  def __init__(self, portBase, httpProxy = None):
  
    self.portBase = portBase
    self.httpProxy = httpProxy
    self.prefixes = [20000,30000,40000,50000]
    self.proc = None
    self.proxyPort = None
    self.proxyUrl = None
    
  def startProxy(self):

    # Port prefixes to try in case some of them are used already
    for prefix in self.prefixes:   
      self.proxyPort = str(prefix+self.portBase)
      commands = ['mitmdump', '--ssl-insecure', '-q', '-p', self.proxyPort, '-s', 'mitmInterceptor.py']
      if(self.httpProxy is not None):
        commands.extend(['--mode','upstream:'+self.httpProxy])
      self.proc = Popen(commands)
      self.proxyUrl = 'localhost:'+self.proxyPort
      time.sleep(0.01)
      poll = self.proc.poll()
      if(poll is not None):
        print("Failed to start proxy: ", self.proxyPort)
        self.proc.kill()
        self.proc = None
        self.proxyPort = None
        self.proxyUrl = None
        time.sleep(1)
      else:
        return True
    print("Proxy couldn't start: " + str(self.portBase))
    return False
    
    
  def sendCommand(self,command):
  
    req = urlrequest.Request(command)
    req.set_proxy(self.proxyUrl , 'http')
    return urlrequest.urlopen(req).read()
    
    
  def sendSimpleCommand(self,command,key,value):
  
    if(key is None):
      response = self.sendCommand('http://'+command+'/').decode()
    else:
      params = urlencode({quote_plus(key):quote_plus(value)})  
      response = self.sendCommand('http://'+command+'/?'+params).decode()
    if(response != 'true'):
      print('Command failed: '+ command)
      return False
    return True
  
    
  def addHeader(self,header,value):
  
    return self.sendSimpleCommand('addHeader',header,value)
    
    
  def removeAddHeader(self,header):
  
    return self.sendSimpleCommand('removeAddHeader',header,'empty')
    
    
  def removeResponse(self):
  
    return self.sendSimpleCommand('removeResponse',None,None)
    
    
  def keepResponse(self):
  
    return self.sendSimpleCommand('keepResponse',None,None)
  
    
  def startHarCollection(self,url):
  
    return self.sendSimpleCommand('startHarCollection','url',url)
  
  
  def getHar(self):
  
    response = self.sendCommand('http://getHar/')
    return json.dumps(pickle.loads(response))
    

  def close(self):
    
    self.proc.kill()

    