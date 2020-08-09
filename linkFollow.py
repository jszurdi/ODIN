from selenium import webdriver
import time
import datetime
import os
import os.path
import random
import json
import sys
from selenium.webdriver.firefox.firefox_profile import AddonFormatError
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.common.exceptions import NoAlertPresentException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import traceback
import logging
import csv
import smtplib

import redirectChainExtractor as rce


def sendEmail(msg):
  
  server = smtplib.SMTP('smtp.gmail.com:587')
  server.ehlo()
  server.starttls()
  server.login('email', 'pwd')
  server.sendmail('email', ['recipient'], msg)
  server.quit()


class LinkFollow:

        
  """
  Initialize 
  proxyInfo[3] is used as and id for LinkFollow, thus always must be given!
  """
  def __init__(self, id ,runConfig, scrapeType, proxy, httpProxy = None):
  
    # Supress urllib3 warnings
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    
    # Different scrape type settings
    self.id = id
    self.name = scrapeType['name']
    self.scrapeType = scrapeType
    self.logTimestamp = 0
    
    self.useragent = getattr(runConfig,scrapeType['ua'])
    if(scrapeType['ref']):
      self.referer = runConfig.referer
    else:
      self.referer = None
    self.isMobile = scrapeType['mobile']
    # Crawl type settings
    self.browserType = scrapeType['browser']
    self.headless = runConfig.headless
    self.timeout = runConfig.timeout
    self.fileWaitTimeout = runConfig.fileWaitTimeout
    self.browserStartRetry = runConfig.browserStartRetry
    self.maxRetry = runConfig.numScrapeRetriesLinkFollower
    # Alert and Download settings:
    self.maxAlerts = runConfig.maxAlerts
    self.waitBetweenAlerts = runConfig.waitBetweenAlerts
    self.downloadFolder = runConfig.downloadFolder+runConfig.day+'/'+str(id) +'/'+self.name+'/'
    if(not os.path.exists(self.downloadFolder)):
      os.makedirs(self.downloadFolder)
    # Other settings    
    self.saveToDisk = runConfig.saveToDisk
    self.extensionsFolder = runConfig.extensionsFolder
    # Web proxy info for network logging
    self.proxy = proxy   
    self.httpProxy = httpProxy
    # Result destination settings  
    if(self.saveToDisk):
      randomString = str(random.randint(1, 1000000))
      self.harFolder = "har_files/" + randomString + "/"
      self.screenshot_folder = "screenshots/" + randomString + "/"
      print(self.harFolder)
      print(self.screenshot_folder)
      if(not os.path.exists(self.harFolder)):
        os.makedirs(self.harFolder)
      if(not os.path.exists(self.screenshot_folder)):
        os.makedirs(self.screenshot_folder)
    # Start Web driver
    if(self.browserType == 'chrome'):
      self.driver = self._startChromeDriver()
    else:
      self.driver is None
    if(self.driver is None):
      return None
    if(self.headless):
      self.enableDownloadInHeadless(self.downloadFolder)
    
    
  """
  Source: https://stackoverflow.com/questions/45631715/downloading-with-chrome-headless-and-selenium
  Function to enable downloads in headless chrome
  """
  def enableDownloadInHeadless(self, downloadFolder):
  
    """
    There is currently a "feature" in chrome where
    headless does not allow file download: https://bugs.chromium.org/p/chromium/issues/detail?id=696481
    This method is a hacky work-around until the official chromedriver support for this.
    Requires chrome version 62.0.3196.0 or above.
    """
    # add missing support for chrome "send_command"  to selenium webdriver
    self.driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': downloadFolder}}
    commandResult = self.driver.execute("send_command", params)

    
  """
  Try to start the chrome driver
  """
  def _startChromeDriver(self):
  
    opts = Options()
    if(True):
      opts.add_argument("--no-sandbox")
      opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("user-agent="+self.useragent)
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--lang=en-US,en")
    opts.add_argument("--start-maximized")
    if(self.proxy is not None):
      opts.add_argument("--proxy-server={0}".format(self.proxy.proxyUrl))
    opts.add_extension(self.extensionsFolder+'Sheets_v1.2.crx')
    opts.add_extension(self.extensionsFolder+'Docs_v0.10.crx')
    
    if(self.isMobile):
      mobileEmulation = {
            "deviceMetrics": {"width": self.scrapeType['width'], 
                              "height": self.scrapeType['height'], 
                              "pixelRatio": self.scrapeType['pixelRatio']},
            "userAgent": self.useragent }
      opts.add_experimental_option("mobileEmulation", mobileEmulation)
    prefs = {'download.default_directory' : self.downloadFolder,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': False,
            'safebrowsing.disable_download_protection': True}
    opts.add_experimental_option('prefs', prefs)
    
    if(self.referer is not None and not self.headless):
      opts.add_extension(self.extensionsFolder+'Referer-Control_v1.32.crx')
    if(self.headless):
      opts.add_argument('headless')
      
    # capabilities
    desiredCaps = Options()
    desiredCaps = desiredCaps.to_capabilities()
    desiredCaps["unexpectedAlertBehaviour"] = "accept"
    desiredCaps['acceptInsecureCerts'] = True
    desiredCaps['loggingPrefs'] = {'performance': 'ALL'}
    
    for i in range(self.browserStartRetry):
      try:
        driver = webdriver.Chrome('prereqs/chromedriver-v74/chromedriver',
                                  chrome_options=opts, desired_capabilities=desiredCaps)
        driver.set_page_load_timeout(self.timeout)
      except Exception as e:
        logging.error(traceback.format_exc())
        print("Error starting browser: ",str(self.id),' - ',self.name)
        try:
          driver.quit()
        except:
          pass
        time.sleep(1)
      else:
        return driver
        
    print("Couldn't create browser: ",str(self.id),' - ',self.name)
    msg = "Couldn't create browser: " + str(self.id) + ' - ' + self.name + ' date: ' + str(datetime.datetime.now())
    sendEmail("Subject: ERROR Starting Browser!!!\n\n"+msg)
    return None  
       
       
  """
  Try to stop driver if object is deleted
  """
  def __del__(self):
  
    try:
      self.driver.quit()
    except:
      pass
    
  
  """
  Try to clean up after previous run
  """
  def cleanup(self):    
  
    try:
      self.driver.delete_all_cookies()
    except Exception as e:
      print("delete_all_cookies failed")
    try:
      self.driver.execute_script('window.localStorage.clear();')
    except Exception as e:
      print("window.localStorage.clear() failed")
    try:
      self.driver.execute_script('window.sessionStorage.clear();')
    except Exception as e:
      print("window.sessionStorage.clear() failed")
     
     
  """
  Set referrer through plug-in, this is not used anymore
  """
  def setReferer(self, url):
  
    self.driver.get("chrome-extension://hnkcfpcejkafcihlgbojoidoihckciin/chrome/content/background.html")
    refSettings = '{"sites":[{"id":"defaultAction","val":"dummy","type":"normal","filter":"","is3rd":true},'
    refSettings += '{"id":1546962179065,"val":"'+url+'","type":"specific","filter":"'+self.referer+'","isregexp":false,"is3rd":false,"isfrom":true,"isto":true},'
    refSettings += '{"id":1546976778522,"val":"","type":"normal","filter":"","isregexp":false,"is3rd":false,"isfrom":true,"isto":true}],"active":true}'
    res = self.driver.execute_script("localStorage.setItem('settings', '"+refSettings+"');")

    
  """
  Filters the performance logs to include only new entries
  Also changes the timestamp
  """
  def filterLog(self,performanceLog):
  
    maxTimestamp = 0
    filtered = []
    for entry in performanceLog:
      ts = entry['timestamp']
      if(ts > self.logTimestamp):
        filtered.append(entry)
      if(ts > maxTimestamp):
        maxTimestamp = ts
    if(maxTimestamp > self.logTimestamp):
      self.logTimestamp = maxTimestamp
    return filtered


  """
  This function handles the appearance of unexpected alerts
  """
  def handleAlerts(self):
    
    notStopped = True
    counter = 0
    while(notStopped and counter < self.maxAlerts):
      try:
        alert = self.driver.switch_to_alert()
        alert.accept()
      except NoAlertPresentException as e:
        notStopped = False
      except Exception as e:
        logging.error(traceback.format_exc())
        notStopped = False
      else:
        counter += 1
        time.sleep(self.waitBetweenAlerts)
   

  def setupScrape(self):
    
    # check if driver is still working return None if not
    if(self.driver is None):
      self.driver = self._startChromeDriver()
      if(self.driver is None):
        return False 
    # Check if proxy crashed    
    if(self.proxy is not None):
      poll = self.proxy.proc.poll()
      if(poll is not None):
        print('Mitmproxy crashed: ',self.id)
        return False
      # Try to set the referer using th plugin if not headless
      if(self.referer is not None):
        self.proxy.addHeader('Referer',self.referer)
      else:
        self.proxy.removeAddHeader('Referer')
    return True


  """
  Updates downloaded file names.
  """
  def updateDownloadedFileNames(self,url):
    
    for f in os.listdir(self.downloadFolder):
      fileName = os.path.join(self.downloadFolder, f)
      if(os.path.isfile(fileName) and not fileName.endswith('.tds.')):
        newFileName = fileName+'.'+str(time.time())+'.tds.'
        os.rename(fileName, newFileName)
        # log the download event
        logFile = self.downloadFolder + 'downloads.log.tds.'
        with open(logFile, mode='a', newline='') as fout:
          writer = csv.writer(fout)
          writer.writerow([newFileName,url])
  
  
  def moveMouse(self, x, y):
  
    try:
      action =  ActionChains(self.driver)
      action.move_by_offset(x,y)
      action.perform()
    except UnexpectedAlertPresentException as e: 
      self.handleAlerts()
      
      
  def wait(self, t):
  
    try:
      time.sleep(t)
    except UnexpectedAlertPresentException as e: 
      self.handleAlerts()
  
          
  def waitAndMoveMouse(self,timeLimit):
  
    try:
      timeLeft = float(timeLimit)
      self.moveMouse(random.randint(40,80),random.randint(40,80))
      for i in range(int(timeLimit)):
        self.moveMouse(int(random.uniform(-3, 3)),int(random.uniform(-3, 3)))
        waitTime = random.uniform(0, 1)
        self.wait(waitTime)
        timeLeft -= waitTime
      if(timeLeft > 0):
        time.sleep(timeLeft)
    except TimeoutException as e:
      print("Page took longer than "+str(self.timeout)+" seconds while time.sleep")
      return False
    except Exception as e:
      logging.error(traceback.format_exc())
      print('Unhandled exception for mouseMove: ',e)
      return False
    else:
      return True
      

  def getVideoToClick(self):
  
    adDomains = set(['google.com','facebook.com','twitter.com'])
    maxVid = None
    maxWidth = 0
    maxType = None
    for tagName in ['embed','video','iframe']:
      for b in self.driver.find_elements_by_xpath("//"+tagName):
        if(b.location['y'] >= 0 and b.location['y'] < 10000 and b.is_displayed()):
          domain = rce.getDomain(b.get_attribute("src"))
          if(b.size['width'] > maxWidth and domain not in adDomains):
            maxVid = b
            maxWidth = b.size['width']
            maxType = tagName
    return maxVid
    
    
  def getLinkToClick(self):
  
    links = {}
    for b in self.driver.find_elements_by_xpath("//a"):
      if(b.is_displayed()):
        href = b.get_attribute("href")
        aDomain = rce.getDomain(href)
        if(aDomain not in links):
          links[aDomain] = []
        links[aDomain].append(b)
    maxDomain = None
    maxLinks = 0
    for domain,hrefs in links.items():
      if(len(hrefs) > 5 and len(hrefs) > maxLinks):
        maxDomain = [domain,hrefs]
        maxLinks = len(hrefs)
    if(maxDomain is not None):
      selectedLink = random.sample(maxDomain[1],1)[0]
      return selectedLink
      
      
  def clickElement(self,element,nClick):
  
    action =  ActionChains(self.driver)
    # Move the mouse to the element
    try:
      action.move_by_offset(random.randint(0,1),random.randint(0,1))
      action.move_to_element(element)
      action.perform()
    except UnexpectedAlertPresentException as e: 
      self.handleAlerts()
    except Exception as e:
      print('Element to move to not found for click number ',nClick,' : ', e)
    # Click the element  
    try:
      action.click()
      action.perform()
    except UnexpectedAlertPresentException as e: 
      self.handleAlerts()
    except Exception as e:
      print('Click ',nClick,' failed: ',e)
    time.sleep(random.uniform(1, 2))
  
  
  def getPerWindowData(self, url, urlBeforeClick):
  
    domain = rce.getDomain(url)
    counter = 0
    perWindowData = {}
    windows = {}
    for handle in self.driver.window_handles:
      counter += 1
      try:
        self.driver.switch_to.window(handle)
      except UnexpectedAlertPresentException as e: 
        self.handleAlerts()
      except Exception as e:
        print('Element to move to not found for click number')
        logging.error(traceback.format_exc())
        
      # Try reloading the current window
      try:
        startUrl = self.driver.current_url
        # We don't want to reload the main page
        if(startUrl == urlBeforeClick):
          continue
        self.driver.refresh()
      except UnexpectedAlertPresentException as e: 
        self.handleAlerts()
      except TimeoutException as e:
        print("Page took longer than "+str(self.timeout)+" seconds, retrying")
        continue
      except Exception as e:
        logging.error(traceback.format_exc())
        print('Unhandled exception for window: ',counter,' and for url: ',url)
        continue
        
      if(not self.waitAndMoveMouse(4)):
        continue
        
      # Save information about the current windows
      try:
        screenshotData = self.driver.get_screenshot_as_png()
        performanceFile = json.dumps(self.filterLog(self.driver.get_log('performance')), ensure_ascii=False)
      except Exception as e:
        print('Saving screenshot/perflog failed: ', e)
        continue
      
      try:
        html = self.driver.find_element_by_tag_name('html').get_attribute('innerHTML')
        html = '<html>'+html+'</html>'
      except Exception as e:
        print('Getting html failed: ', e)
        html = self.driver.page_source   
      currentUrl = self.driver.current_url
      perWindowData[counter] = [screenshotData,html,startUrl,currentUrl,performanceFile]
      randomString = str(random.randint(1, 1000000))
      
    return perWindowData
  

  def selectAndClickElement(self,url):
    
    try:
      element = self.getVideoToClick()
    except:
      element =  None
      
    # If video is not found click a link if there are many video links
    if(element is None):
      try:
        element = self.getLinkToClick()
      except:
        element = None
    if(element is None):
      return None
    urlBeforeClick = self.driver.current_url
    self.proxy.removeResponse()
    self.clickElement(element,1)
    self.clickElement(element,2)
    self.proxy.keepResponse()
    time.sleep(1)
    return self.getPerWindowData(url, urlBeforeClick)
  

  def getPage(self, url):
  
    try:
      if(self.proxy is not None):
        self.proxy.startHarCollection(url)
      self.driver.get(url)
    except TimeoutException as e:
      print("Page took longer than "+str(self.timeout)+" seconds, retrying")
      return False
    except UnexpectedAlertPresentException as e:
      try:
        print(e)
        print("Trying to handle UnexpectedAlertPresentException1")
        self.handleAlerts()
        time.sleep(self.fileWaitTimeout)
      except Exception as e:
        logging.error(traceback.format_exc())
        print("Couldn't handle UnexpectedAlertPresentException1",url)
        return False
      else:
        return True
    except Exception as e:
      logging.error(traceback.format_exc())
      print('Unhandled exception for url: ',url)
      return False
    else:
      return True
 

  def saveDataToDisk(self,data):

    ts, url, landingUrl, html, harFile, screenshotData, performanceFile, secondaryData = data
    
    # Save primary data
    randomString = str(random.randint(1, 1000000))
    with open(self.screenshot_folder + randomString + "-screenshot.png", mode = 'wb') as fout:
      fout.write(screenshotData)
    with open(self.harFolder+randomString+"-test.har", mode = 'wb') as fout:
      fout.write(harFile.encode('utf8','ignore'))
    with open(self.harFolder+randomString+"-performance.log", mode = 'wb') as fout:
      fout.write(performanceFile.encode('utf8','ignore'))
    with open(self.harFolder+randomString+"-main.html", mode = 'wb') as fout:
      fout.write(html.encode('utf8','ignore'))
    with open(self.harFolder+randomString+"-mainUrls.txt", mode = 'w') as fout:
      csvwriter = csv.writer(fout)
      csvwriter.writerow([url,landingUrl])
      
    # Save after click data
    if(secondaryData is None):
      return
    with open(self.harFolder+randomString+"-afterclick.har", mode = 'wb') as fout:
      fout.write(secondaryData['har'].encode('utf8','ignore'))
    flog = open(self.screenshot_folder+randomString+"-afterclick-urls.txt", mode = 'w')
    csvwriter = csv.writer(flog)
    if(secondaryData['perWindowData'] is not None):
      for key,item in secondaryData['perWindowData'].items():
        with open(self.harFolder+randomString+"-afterclick-window"+str(key)+".html", mode = 'wb') as fout:
          fout.write(item[1].encode('utf8','ignore'))
        with open(self.screenshot_folder + randomString + "-afterclick-window"+str(key)+"-screenshot.png", mode = 'wb') as fout:
          fout.write(item[0])   
        with open(self.harFolder+randomString + "-afterclick-window"+str(key)+"-performance.log", mode = 'wb') as fout:
          fout.write(item[4].encode('utf8','ignore'))
        csvwriter.writerow(['window'+str(key),item[2],item[3]])
      flog.close()
      
    
  """
  Follow a url and scrape page
  """
  def followLink(self, url, doUserAction = False):
  
    # Check if driver and proxy are still able to collect the page
    if(not self.setupScrape()):
      return None
    
    # Making sure we have time for user action
    if(doUserAction):    
      self.driver.set_page_load_timeout(self.timeout*2)
    else:
      self.driver.set_page_load_timeout(self.timeout)
      
    # Clean up after previous run
    try:
      self.driver.delete_all_cookies()
    except Exception as e:
      print("Delete_all_cookies at the beginning failed")

    # Try maxRetry number of time to collect the page
    for i in range(self.maxRetry):
      if(self.getPage(url)):
        self.waitAndMoveMouse(self.fileWaitTimeout)
        break
      self.waitAndMoveMouse(self.fileWaitTimeout)
      
    # Getting the data from scraping should succeed or fail together
    try:
      screenshotData = self.driver.get_screenshot_as_png()
      performanceFile = json.dumps(self.filterLog(self.driver.get_log('performance')), ensure_ascii=False)
      
      if(self.proxy is not None):
        harFile = self.proxy.getHar()
      else:
        harFile = ''
      try:
        html = self.driver.find_element_by_tag_name('html').get_attribute('innerHTML')
        html = '<html>'+html+'</html>'
      except Exception as e:
        print('Getting html failed: ', e)
        html = self.driver.page_source    
    except TimeoutException as e:
      print("Page took longer than "+str(self.timeout)+" seconds, retrying")
      return (time.time(), "", "", "", "", "", "", "", "", "")
    except Exception as e:
      logging.error(traceback.format_exc())
      print("Some sort of exception while loading and post processing")
      return (time.time(), "", "", "", "", "", "", "", "", "")
    landingUrl = self.driver.current_url
    # If we got this far and we want to do user action then do it now
    if(doUserAction):
      if(self.proxy is not None):
        self.proxy.startHarCollection(url)
      perWindowData = self.selectAndClickElement(url)
      if(self.proxy is not None):
        harFilSecondary = self.proxy.getHar()
      else:
        harFileSecondary = ''
      secondaryData = {'perWindowData':perWindowData,'har':harFilSecondary}
    else:
      secondaryData = None

    data = (time.time(), url, landingUrl, html, harFile, screenshotData, performanceFile, secondaryData, self.httpProxy)    
    # Fix download file names
    self.updateDownloadedFileNames(url)  
    # Save info to disk    
    if(self.saveToDisk):
      self.saveDataToDisk(data)
      
    # Finally return results
    return data

