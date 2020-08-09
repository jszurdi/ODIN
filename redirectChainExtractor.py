"""
This module can be used to extract redirection chains from HAR files and performance logs.
"""
import re
import json
from haralyzer import HarParser, HarPage
from bs4 import BeautifulSoup
from bs4.element import Comment
import tldextract
from urllib.parse import urlparse, parse_qsl
from urllib.parse import urljoin, unquote_plus
from urllib.parse import urldefrag
from copy import deepcopy
from datetime import datetime
import time
import csv
import copy
from timeout import timeout



"""
RedirectPair object are what redirections made out of
"""
class RedirectPair:


  def __init__(self,src,target,type):
  
    self.src = src
    self.target = target
    self.type = type

    
  def __str__(self):
  
    return str(self.src)+' -> '+str(self.target)+' ('+str(self.type)+')'


"""
A url object that can be compared with other url objects
without regard to the vagaries of encoding, escaping, and ordering
of parameters in query strings.
"""   
class Url(object):


  def __init__(self, url):
  
    if(url is None):
      raise TypeError
    else:
      url = urldefrag(url).url
      parts = urlparse(url)
      _query = frozenset(parse_qsl(parts.query))
      _path = unquote_plus(parts.path)
      if(_path.endswith('/')):
        _path = _path[:-1]
      parts = parts._replace(query=_query, path=_path)
      self.url = url
      self.parts = parts

    
  def __eq__(self, other):
  
    if(other is None):
      return False
      
    return self.parts == other.parts

    
  def __hash__(self):
  
    return hash(self.parts)
    
    
  def __str__(self):
  
    return self.url
      
      
  def __repr__(self):
  
    return self.url

    
"""
Makes sure if url string is None then the Url object should be None too
"""    
def getUrl(url):
  
  if(url is None):
    return None
  else:
    return Url(url)

    
"""
Check if a HAR entry is an HTML file
"""   
def isHtml(entry):
  
  lowerCaseEntry = json.loads(json.dumps(entry, ensure_ascii=False).lower())
  headers = {x['name']:x for x in lowerCaseEntry['response']['headers']}
  if(('content-type' in headers) 
    and re.search('html',headers['content-type']['value'],re.IGNORECASE)
    and ('text' in lowerCaseEntry['response']['content'])):
    return True
  else:
    return False
    
"""
Return soupified version of html from a HAR entry if it is an html file
"""    
def getHtml(entry):

  if(isHtml(entry) and 'text' in entry['response']['content']):
    return BeautifulSoup(entry['response']['content']['text'],features="html.parser")
 

      
"""
Can be used to load HAR and performance log files
"""
def loadFile(filein):

  with open(filein, encoding='utf-8') as fin:
    return json.loads(fin.read())
  

"""
Retrieves a header value given by "key" from an entry.
Type is either request or response.
"""
def getHeaderValue(entry,key,type):

  ourReferer = 'https://www.google.com'
  for field in entry[type]['headers']:
    if(field['name'].lower() == key.lower() and field['value'] != ourReferer):
      return field['value']
  return None
  
  
"""
Retrieves a header value given by "key" from an http response.
"""
def getResponseHeaderValue(entry,key):

  return getHeaderValue(entry,key,'response')
  
    
"""
Retrieves a header value given by "key" from an http request.
"""
def getRequestHeaderValue(entry,key):

  return getHeaderValue(entry,key,'request')
 

"""
Retrieves HTTP redirects from an entry based on headers
"""
def getHttpRedirect(entry,baseUrl):

  # 1.1. 3xx + location based redirect
  if(str(entry['response']['status']).startswith('3')):
    location = getResponseHeaderValue(entry,'location')
    if(location is not None):
      return getUrl(urljoin(baseUrl,location))
    else:
      return Url('error')
  # 1.2. http header refresh redirect
  # Refresh http header redirect might not need a 3xx code
  refresh = getResponseHeaderValue(entry,'refresh')
  if(refresh is not None):
    if(';' in refresh):
      refresh = ';'.join(refresh.split(';')[1:]).strip()
    if('=' in refresh):
      refresh = '='.join(refresh.split('=')[1:]).strip()
    return getUrl(urljoin(baseUrl,refresh))


"""
Get meta redirect from HTML header
"""
def getMetaRedirect(entry,soup,baseUrl):

  metaRedir = soup.find('meta', attrs={'http-equiv':re.compile('^\s*refresh\s*$', re.I)})
  if((metaRedir is not None)
    and (metaRedir.has_attr('content'))
    and (len(metaRedir['content'].split('=')) > 1)):
    url = '='.join(metaRedir['content'].split('=')[1:]).strip('"').strip("'")
    return getUrl(urljoin(baseUrl,url))

    
"""
Checks if a string is a valid javascript variable
"""
def isJsVar(string):

  if(len(string) < 1):
    return False
  else:
    return not bool(re.compile(r'[^a-zA-Z0-9$_]').search(string))
    
    
"""
Finds a value for a javascript variable "varName" in soup if the value is a simple string
"""
def findValue(varName,baseUrl,soup,entries):

  # check if it is one variable name
  if(isJsVar(varName)):
    value = None
    redirMatchs = re.finditer(varName+r"""\s*=\s*\"([^']+?)\"""", soup, re.I|re.M|re.S)
    for redirMatch in redirMatchs:
      value = redirMatch.group(1)
    redirMatchs = re.finditer(varName+r"""\s*=\s*\'([^"]+?)\'""", soup, re.I|re.M|re.S)
    for redirMatch in redirMatchs:
      value = redirMatch.group(1)
    if(value is not None):
      return getUrl(urljoin(baseUrl,value))
    else:
      return Url('error')
  # check for a common string often leading to malicious redirects of form x = a + b + c + ...
  elif(re.match(r"""\s*?a\s*?\+.*?""", varName, re.I|re.M|re.S)):
    subUrl = findValue('a',baseUrl,soup,entries)
    if(str(subUrl) == 'error'):
      return Url('error')
    else:
      for url in entries:
        for entry in entries[url]:
          urlStr = str(url)
          if(urlStr.startswith(str(subUrl))):
            return url
  else:
    return Url('error')
  

"""
This module attempts to find javascript redirects using regular expressions from a soup file.
This module cannot handle:
 - race conditions (asynchronous javascript)
 - coded or escaped javascript code
 - if conditions
 - convoluted variable assignments
 - probably many other cases
 
These problems are corrected by using performance logs from chrome.
"""
def getJavascriptRedirectFromSoup(baseUrl,soup,entries):

  redirUrls = []
  try:
    soup = str(soup)
  except:
    return redirUrls
  # (window|document)? .?  location .? (href)? = 'URL'
  redirMatchs = re.finditer(r"""(window|document)?\.?(top\.)?location\.?(href)?\s*=\s*\(?\s*\'([^']+)\'\s*\)?""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    redirUrls.append(getUrl(urljoin(baseUrl,redirMatch.group(4))))
  # (window|document)? .?  location .? (href)? = "URL"
  redirMatchs = re.finditer(r"""(window|document)?\.?(top\.)?location\.?(href)?\s*=\s*\(?\s*\"([^"]+)\"\s*\)?""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    redirUrls.append(getUrl(urljoin(baseUrl,redirMatch.group(4))))
  # if (window|document)? .?  location .? (href)? = variables or expression
  redirMatchs = re.finditer(r"""(window|document)?\.?(top\.)?location\.?(href)?\s*=\s*(.*?)(\s|;)""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    if(getUrl(urljoin(baseUrl,redirMatch.group(4).strip('"').strip("'"))) not in redirUrls):
      redirUrls.append(findValue(redirMatch.group(4),baseUrl,soup,entries))
  redirMatchs = re.finditer(r"""(window|document)?\.?(top\.)?location\.(replace|assign|redirect)\s*?\(\s*(\"|\')([^"']+)(\"|\')\s*\)""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    redirUrls.append(getUrl(urljoin(baseUrl,redirMatch.group(5))))
  # (window|document)? .?  location . (replace|assign|redirect).\(variable or expression\)
  redirMatchs = re.finditer(r"""(window|document)?\.?(top)?\.?location\.(replace|assign|redirect)\s*?\(\s*(.*?)\s*\)""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    if(getUrl(urljoin(baseUrl,redirMatch.group(4).strip('"').strip("'"))) not in  redirUrls):
      redirUrls.append(findValue(redirMatch.group(4),baseUrl,soup,entries))
  redirMatchs = re.finditer(r"""\$\(\s*location\s*\)\.attr\s*?\(\s*(\"|\')?href(\"|\')?\s*\,\s*(\"|\')?([^"']+)(\"|\')?\s*\)""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    redirUrls.append(getUrl(urljoin(baseUrl,redirMatch.group(4))))
  # $(location).attr('href', vaiable or expression);
  redirMatchs = re.finditer(r"""\$\(\s*location\s*\)\.attr\s*?\(\s*(\"|\')?href(\"|\')?\s*\,\s*(.*?)\s*\)""", soup, re.I|re.M|re.S)
  for redirMatch in redirMatchs:
    if(getUrl(urljoin(baseUrl,redirMatch.group(3).strip('"').strip("'"))) not in  redirUrls):
      redirUrls.append(findValue(redirMatch.group(3),baseUrl,soup,entries))
  return redirUrls
  
    
"""
This module retrieves all javascript files included in an entry to find javascript redirects.
"""
def getJavascriptRedirect(entry,soup,entries,baseUrl):

  redirUrls = []
  # 3.1 look in internal javascript
  redirUrls.extend(getJavascriptRedirectFromSoup(baseUrl,soup,entries))
  
  # 3.2. look in external javascript files
  scripts = soup.find_all('script')
  for script in scripts:
    if(script.has_attr('src')):
      scriptSrc = getUrl(urljoin(baseUrl,script['src']))
      if(scriptSrc in entries):
        for entry in entries[scriptSrc]:
          if('text' in entry['response']['content']):
            scriptSoup = BeautifulSoup(entry['response']['content']['text'],features="html.parser")
            redirUrls.extend(getJavascriptRedirectFromSoup(baseUrl,scriptSoup,entries))
  return redirUrls


"""
Check if a HTML tag is visible to the user.
"""
def isTagVisible(element):

  if(element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']):
    return False
  if(isinstance(element, Comment)):
    return False
  return True    

    
"""
Retrieves text visible to the user.
"""
def getVisibleText(soup):

  texts = soup.findAll(text=True)
  visibleTexts = filter(isTagVisible, texts)  
  return u" ".join(t.strip() for t in visibleTexts)
  
  
"""
Gets HTML tags redirecting users or including content from another url:
 - Frame redirects: tagNames = ['iframe', 'frame'] and field = 'src'
 - Form redirects: tagNames = ['form'] and field = 'action'
This module makes sure that the new content is significant to include it in the redirection.
"""
def getTagRedirect(entry,soup,entries,baseUrl,tagNames,field,nextSrc,domContentDistribution):

  mainDomContent = domContentDistribution[getDomain(nextSrc)]
  # Get main page stats
  lenMainVisibleText = len(getVisibleText(soup))
  body = soup.find('body')
  if(body is not None):
    try:
      lenMainCodeBody = len(str(body))
    except:
      return []
  else:
    lenMainCodeBody = 0
    
  redirUrls = set()
  # Find tags where they provide the majority of the content 
  # and they have a field which is a URL in entries
  tags = soup.find_all(tagNames)
  for tag in tags:
    if(tag.has_attr(field)):
      tagUrl = getUrl(urljoin(baseUrl,tag[field]))
      if(tagUrl in entries):
        tagDomContent = domContentDistribution[getDomain(tagUrl)]
        for entry in entries[tagUrl]:
          if('text' in entry['response']['content']):
            tagSoup = BeautifulSoup(entry['response']['content']['text'],features="html.parser")
            # Get tag stats
            lenTagVisibleText = len(getVisibleText(tagSoup))
            try:
              lenTagCodeBody = len(str(tag))
            except:
              continue
            # if the tag represents more visible content of more code in the html body
            # then we assume the tag is the main content of the page
            if((lenMainVisibleText < lenTagVisibleText) or
                (lenMainCodeBody < lenTagCodeBody*2) or
                (mainDomContent*0.4 < tagDomContent)):
              redirUrls.add(tagUrl)
  return redirUrls
    

"""
Tries to retrieve redirects in all possible ways a page can redirect from one soup.
At the moment it guesses the first potential redirect encountered as the redirect the page tries to do.
"""
def getRedirect(entry,soup,entries):
    
  # 0. get base url
  baseUrl = entry['request']['url']
  if(soup is not None):
    base = soup.find('base')
    if((base is not None) and base.has_attr('href')):
      baseUrl = urljoin(baseUrl,base['href'])

  # 1. check for http redirect
  httpRedirect = getHttpRedirect(entry,baseUrl)
  if(httpRedirect is not None):
    return (['http'], [httpRedirect])
  if(soup is not None):
    redirects = []
    redirectTypes = []
    # 2. check for meta tag redirect
    metaRedirect = getMetaRedirect(entry,soup,baseUrl)
    if(metaRedirect is not None):
      redirects.append(metaRedirect)
      redirectTypes.append('meta')
    # 3. check for javascript redirect
    redirUrls = getJavascriptRedirect(entry,soup,entries,baseUrl)
    if(len(redirUrls) > 0):
      redirects.extend(redirUrls)
      redirectTypes.extend(['javascript'] * len(redirUrls))
    # 4. frame content inclusion
    redirUrls = getTagRedirect(entry,soup,entries,baseUrl,['iframe', 'frame'],'src')
    if(len(redirUrls) > 0):
      redirects.extend(redirUrls)
      redirectTypes.extend(['frame'] * len(redirUrls))
    # 5. form redirect
    redirUrls = getTagRedirect(entry,soup,entries,baseUrl,['form'],'action')
    if(len(redirUrls) > 0):
      redirects.extend(redirUrls)
      redirectTypes.extend(['form'] * len(redirUrls))
    return redirectTypes, redirects
      
  # 6. no redirect
  return (None, None)

  
"""
Retrieves all possible redirection chain elements from all entries in a har file.
"""
def getRedirectSubChains(har):
  
  redirectPairs = {}
  inverseRedirectPairs = {}
  referers = {}
  inverseReferers = {}
  contentDistribution = {}
  errorCodes = {}
  htmlUrls = set()
  entries = {}
  
  har_parser = HarParser(har)  
  for page in har_parser.pages:
    entries = getEntries(page)
    for url, ents in entries.items():
      for entry in ents:
        referer = getUrl(getRequestHeaderValue(entry,'referer'))
        if(referer is not None):
          if(referer not in referers):
            referers[referer] = []
          referers[referer].append(RedirectPair(referer,url,'referer'))
          if(url not in inverseReferers):
            inverseReferers[url] = []
          inverseReferers[url].append(RedirectPair(referer,url,'referer'))
        contentDistribution[url] = entry['response']['bodySize']
        errorCodes[url] = str(entry['response']['status'])
        soup = getHtml(entry)
        if(soup is not None):
          htmlUrls.add(url)
        redirectTypes, targetUrls = getRedirect(entry,soup,entries)
        targetsAdded = set()
        if(targetUrls is None):
          targetUrl = Url('error')
        else:
          for i in range(len(targetUrls)):
            targetUrl = targetUrls[i]
            redirType = redirectTypes[i]
            if(redirType is not None and targetUrl not in targetsAdded):
              targetsAdded.add(targetUrl)
              if(url not in redirectPairs):
                redirectPairs[url] = []
              redirectPairs[url].append(RedirectPair(url,targetUrl,redirType))
              if(targetUrl not in inverseRedirectPairs):
                inverseRedirectPairs[targetUrl] = []
              inverseRedirectPairs[targetUrl].append(RedirectPair(url,targetUrl,redirType))
  
  return (redirectPairs, inverseRedirectPairs, referers, 
          inverseReferers, contentDistribution, htmlUrls, 
          errorCodes, entries)
  

"""
Retrieves the registered domain from an url.
If it errors it returns in this order:
 - hostname/ip
 - the original url
"""
def getDomain(url):

  if(not isinstance(url,str)):
    url = str(url)

  if(url is not None and str(url) != 'error'):
    try:
      hostname = urlparse(str(url.strip())).hostname
      domain = tldextract.extract(hostname).registered_domain.lower()
      if(domain == ''):
        domain = hostname
    except:
      domain = str(url)
    return domain
  else:
    return str(url)

    
"""
Transforms a redirection chain into a domain redirection chain.
"""
def getDomainRedirectionChain(redirectionChain):

  domRedirChain = []
  previousDomain = '-1'
  for redirectPair in redirectionChain:
      srcDomain = getDomain(redirectPair.src)
      targetDomain = getDomain(redirectPair.target)
      if(targetDomain != previousDomain and targetDomain != 'error' and targetDomain is not None):
        domRedirChain.append(RedirectPair(srcDomain,targetDomain,redirectPair.type))
        previousDomain = targetDomain
  return domRedirChain
    
"""
Transforms a redirection chain into a domain redirection chain.
Dicts instead of redirectPair objects
"""
def getDomainRedirectionChainDict(redirectionChain):

  domRedirChain = []
  previousDomain = '-1'
  for redirectPair in redirectionChain:
      srcDomain = getDomain(redirectPair['src'])
      targetDomain = getDomain(redirectPair['target'])
      if(targetDomain != previousDomain and targetDomain != 'error' and targetDomain is not None):
        domRedirChain.append({'src':srcDomain,'target':targetDomain,'type':redirectPair['type']})
        previousDomain = targetDomain
  return domRedirChain
  
    
"""
Gets the per domain name content distribution from  the per url content distribution.
"""
def getPerDomainContent(contentDistribution):

  domains = {}
  for url, size in contentDistribution.items():
    domain = getDomain(url)
    if(domain not in domains):
      domains[domain] = 0
    domains[domain] += size
  return domains

  
"""
Gets the domain which is used the most frequently as an outgoing link.
"""
def getMostFrequentOutgoingDomain(entries):

  domains = {}
  for entry in entries:
    # Get base url
    baseUrl = entry['request']['url']
    soup = getHtml(entry)
    if(soup is not None):
      base = soup.find('base')
      if((base is not None) and base.has_attr('href')):
        baseUrl = urljoin(baseUrl,base['href'])

    if(soup is not None):
      for link in soup.findAll('a'):
        if(link.has_attr('href')):
          try:
            domain = getDomain(getUrl(urljoin(baseUrl,link['href'])))
            if(domain not in domains):
              domains[domain] = 0
            domains[domain] += 1
          except Exception as e:
            print(e)
  if(len(domains) > 0):
    maxDomain = max(domains, key=domains.get)
    return maxDomain, domains[maxDomain]
  else:
    return None, None
  
  
"""
If there was no redirection then tests if the content of the page is coming from another domain.
"""
def guessContentInclusion(redirectionChain,entries,contentDistribution,domContentDistribution):

  if(len(redirectionChain) == 1 and redirectionChain[-1].target in entries):
    domain, outFrequency = getMostFrequentOutgoingDomain(entries[redirectionChain[-1].target])
    domainContents = domContentDistribution
    originalDomain = getDomain(redirectionChain[-1].target)
    originalDomainContent = domainContents[originalDomain] if originalDomain in domainContents else 0
    domainContent = domainContents[domain] if domain in domainContents else 0
    domains = set([getDomain(x.target) for x in redirectionChain])
    if(domain is not None and originalDomain is not None and originalDomain != domain and 
      outFrequency > 10 and domainContent > originalDomainContent*2 and domain not in domains):
      redirectionChain.append(RedirectPair(redirectionChain[-1].target,getUrl('http://'+domain),'inclusion'))
      redirectionChain[-1].responseFound = True
   

"""
Extracts the first redirect from a redir pair where the target was in entries.
If not then just returns the first redir pair found.
"""
def extractNextHopfromRedirPairs(redirPairList,entries):

  # first tries to retrieve a redirect that was actually found in an entry
  for redirPair in redirPairList:
    if(redirPair.target in entries):
      return redirPair
  # if no such redir is found then return the first target found
  return redirPairList[0]
   

"""
Extracts the first redirect from a redir pair where the src was in entries.
If not then just returns the first redir pair found.
"""
def extractNextHopfromReverseRedirPairs(redirPairList,entries):

  # first tries to retrieve a redirect that was actually found in an entry
  for redirPair in redirPairList:
    if(redirPair.src in entries):
      return redirPair
  # if no such redir is found then return the first target found
  return redirPairList[0]
  

"""
Remove the redirPair used in the redirectChain from redirectPairs and inverseRedirectPairs
"""
def removeRedirPair(newRedir,redirectPairs,inverseRedirectPairs):
  
  src = newRedir.src
  target = newRedir.target
  # Remove from redirectPairs
  for i in range(len(redirectPairs[src])):
    if(redirectPairs[src][i].target == target):
      del redirectPairs[src][i]
      break
  if(len(redirectPairs[src]) == 0):
    del redirectPairs[src]
  # Remove from inverseRedirectPairs
  for i in range(len(inverseRedirectPairs[target])):
    if(inverseRedirectPairs[target][i].src == src):
      del inverseRedirectPairs[target][i]
      break
  if(len(inverseRedirectPairs[target]) == 0):
    del inverseRedirectPairs[target]
 

"""
Tries to reconstruct the redirection chain in a forward manner.
"""
def buildForwardRedirChain(startUrl,redirectPairs,inverseRedirectPairs,entries,maxLooping):

  redirectionChain = [RedirectPair(None,startUrl,'start')]
  endOfChain = False
  # four time looping allowed of same URL
  looped = {}
  while(not endOfChain):
    # Stop loops
    if(redirectionChain[-1].target in redirectPairs and (len(looped) == 0 or max(looped.values()) < maxLooping)):
      newRedir = extractNextHopfromRedirPairs(redirectPairs[redirectionChain[-1].target],entries)
      redirectionChain.append(newRedir)
      removeRedirPair(newRedir,redirectPairs,inverseRedirectPairs)
      if(newRedir.target in [x.src for x in redirectionChain]):
        if(newRedir.target not in looped):
          looped[newRedir.target] = 0
        looped[newRedir.target] += 1
    else:
      endOfChain = True
  return redirectionChain
  
 
"""
Tries to reconstruct the redirection chain in a backward manner.
"""
def buildReverseRedirChain(guessedLastUrl,redirectPairs,inverseRedirectPairs,entries,maxLooping):

  reverseChain = [RedirectPair(guessedLastUrl,None,'end')]   
  endOfChain = False
  looped = {}
  while(not endOfChain):
    # Stop loops
    if(reverseChain[-1].src in inverseRedirectPairs and (len(looped) == 0 or max(looped.values()) < maxLooping)):
      newInverseRedir = extractNextHopfromReverseRedirPairs(inverseRedirectPairs[reverseChain[-1].src],entries)
      reverseChain.append(newInverseRedir)
      removeRedirPair(newInverseRedir,redirectPairs,inverseRedirectPairs)
      if(newInverseRedir.src in [x.target for x in reverseChain]):
        if(newInverseRedir.src not in looped):
          looped[newInverseRedir.src] = 0
        looped[newInverseRedir.src] += 1
    else:
      endOfChain = True
  # Append the reverse chain
  reverseChain.reverse() 
  return reverseChain
  
 
"""
Tries to reconstruct the redirection chain in a backward manner.
"""
def buildReverseRedirChainUsingReferer(landingUrl,redirectPairs,inverseRedirectPairs,entries,referers,inverseReferers,maxLooping):

  reverseChain = [RedirectPair(landingUrl,None,'end')]   
  endOfChain = False
  looped = {}
  while(not endOfChain):
    lastSrc = reverseChain[-1].src
    # Stop loops
    if(lastSrc in inverseRedirectPairs and (len(looped) == 0 or max(looped.values()) < maxLooping)):
      newInverseRedir = extractNextHopfromReverseRedirPairs(inverseRedirectPairs[lastSrc],entries)
      nextSrc = newInverseRedir.src
      #If the next reverse redirect is not found in entries then try if we can find one in referers
      if(nextSrc not in entries and lastSrc in inverseReferers):
        refererRedir = extractNextHopfromReverseRedirPairs(inverseReferers[lastSrc],entries)
        if(refererRedir.src in entries):
          newInverseRedir = refererRedir
          removeRedirPair(newInverseRedir,referers,inverseReferers)
        else:
          removeRedirPair(newInverseRedir,redirectPairs,inverseRedirectPairs)
      else:  
        removeRedirPair(newInverseRedir,redirectPairs,inverseRedirectPairs)
      reverseChain.append(newInverseRedir)
      if(newInverseRedir.src in [x.target for x in reverseChain]):
        if(newInverseRedir.src not in looped):
          looped[newInverseRedir.src] = 0
        looped[newInverseRedir.src] += 1
    else:
      endOfChain = True
  # Append the reverse chain
  reverseChain.reverse() 
  return reverseChain
  
         
  
"""
Gets entries from a HAR file
"""
def getEntries(page):

  entries = {}
  for entry in page.entries:
    url = getUrl(entry['request']['url'])
    if(url not in entries):
      entries[url] = []
    entries[url].append(entry)
  return entries
  

"""
Adds a new redir pair to a collection of redirects
"""
def addRedirToDict(logDict,src,target,requestId):
  
  src = getUrl(src)
  target = getUrl(target)
  if(src not in logDict):
    logDict[src] = []
  logDict[src].append({'src':src,'target':target,'requestId':requestId})
  

"""
Adds a new redir pair to a collection of redirects
"""
def addReverseRedirToDict(logDict,src,target,requestId):
  
  src = getUrl(src)
  target = getUrl(target)
  if(target not in logDict):
    logDict[target] = []
  logDict[target].append({'src':src,'target':target,'requestId':requestId})    
 
 
"""
Gets potential redirects from performance log file
"""
def processPerformanceLog(performanceLog,startUrl):

  # Dictionaries to collect redirection related information
  refererRedirPairs = {}
  redirPairs = {}
  httpRedirects = {}
  reverseRefererRedirPairs = {}
  reverseRedirPairs = {}
  reverseHttpRedirects = {}
  responses = {}
  otherRedirects = {}
  
  frameId = None
  counter = -1
  for log in performanceLog:
    counter += 1
    if(isinstance(log['message'], str)):
      log['message'] = json.loads(log['message'])
    method = log['message']['message']['method']
    params = log['message']['message']['params']
    currentFrameId = None
    if('frameId' in params):
      currentFrameId = params['frameId']
    elif('id' in params):
      currentFrameId = params['id']
    else:
      continue
    
    # gets starting frameId
    if(frameId is None and method == 'Network.requestWillBeSent' and getUrl(params['request']['url']) == startUrl):
      frameId = params['frameId']
      
    # Checks if we are in the right frame
    if(frameId is None or frameId != currentFrameId):
      continue
      
    # Get http redirects
    if('redirectResponse' in params):
      addRedirToDict(httpRedirects,params['redirectResponse']['url'],params['request']['url'],None) 
      addReverseRedirToDict(reverseHttpRedirects,params['redirectResponse']['url'],params['request']['url'],None) 
    
    # Following redirects through requests and responses
    elif(method == 'Network.requestWillBeSent'):
      if('Referer' in params['request']['headers']):
        addRedirToDict(refererRedirPairs,params['request']['headers']['Referer'],params['request']['url'],params['requestId'])
        addReverseRedirToDict(reverseRefererRedirPairs,params['request']['headers']['Referer'],params['request']['url'],params['requestId'])
      # # This can be used as an alternative to referrers
      if('initiator' in params and params['initiator']['type'] == 'script' and 'stack' in params['initiator']):
        for frame in params['initiator']['stack']['callFrames']:
          if(frame == '...'):  # Only for broken performance logs from phone
            continue
          if('stack' in params['initiator'] and frame['url'] != ''):
            addRedirToDict(redirPairs,frame['url'],params['request']['url'],params['requestId'])
            addReverseRedirToDict(reverseRedirPairs,frame['url'],params['request']['url'],params['requestId'])
      elif('initiator' in params and params['initiator']['type'] == 'parser'):
        addRedirToDict(redirPairs,params['initiator']['url'],params['request']['url'],params['requestId'])
        addReverseRedirToDict(reverseRedirPairs,params['initiator']['url'],params['request']['url'],params['requestId'])
    
    # Collecting responses received    
    elif(method == 'Network.responseReceived'):
      url = getUrl(params['response']['url'])
      if(url not in responses):
        responses[url] = set()
      responses[url].add(params['requestId'])
    
    # Meta, post and javascript redirects
    elif(method == 'Page.frameScheduledNavigation'):
      otherRedirects[getUrl(params['url'])] = params['reason']
  
  return (refererRedirPairs, redirPairs, responses, otherRedirects, httpRedirects, 
          reverseRefererRedirPairs, reverseRedirPairs, reverseHttpRedirects)
 
 

def processPerformanceLogAds(performanceLog):

  # Dictionaries to collect redirection related information
  allPairs = set()
  refererRedirPairs = {}
  redirPairs = {}
  httpRedirects = {}
  reverseRefererRedirPairs = {}
  reverseRedirPairs = {}
  reverseHttpRedirects = {}
  responses = {}
  otherRedirects = {}
  
  counter = -1
  for log in performanceLog:
    counter += 1
    if(isinstance(log['message'], str)):
      log['message'] = json.loads(log['message'])
    method = log['message']['message']['method']
    params = log['message']['message']['params']

    # Get http redirects
    if('redirectResponse' in params):
      if(params['redirectResponse']['url']+params['request']['url'] not in allPairs):
        allPairs.add(params['redirectResponse']['url']+params['request']['url'])
        addRedirToDict(httpRedirects,params['redirectResponse']['url'],params['request']['url'],None) 
        addReverseRedirToDict(reverseHttpRedirects,params['redirectResponse']['url'],params['request']['url'],None) 
    
    # Following redirects through requests and responses
    elif(method == 'Network.requestWillBeSent'):
      if('Referer' in params['request']['headers']):
        if(params['request']['headers']['Referer']+params['request']['url'] not in allPairs):
          allPairs.add(params['request']['headers']['Referer']+params['request']['url'])
          addRedirToDict(refererRedirPairs,params['request']['headers']['Referer'],params['request']['url'],params['requestId'])
          addReverseRedirToDict(reverseRefererRedirPairs,params['request']['headers']['Referer'],params['request']['url'],params['requestId'])
      # # This can be used as an alternative to referrers
      if('initiator' in params and params['initiator']['type'] == 'script' and 'stack' in params['initiator']):
        for frame in params['initiator']['stack']['callFrames']:
          if(frame == '...'): # Only for broken performance logs from phone
            continue
          if('stack' in params['initiator'] and frame['url'] != ''):
            if(frame['url']+params['request']['url'] not in allPairs):
              allPairs.add(frame['url']+params['request']['url'])
              addRedirToDict(redirPairs,frame['url'],params['request']['url'],params['requestId'])
              addReverseRedirToDict(reverseRedirPairs,frame['url'],params['request']['url'],params['requestId'])
      elif('initiator' in params and params['initiator']['type'] == 'parser'):
        if(params['initiator']['url']+params['request']['url'] not in allPairs):
          allPairs.add(params['initiator']['url']+params['request']['url'])
          addRedirToDict(redirPairs,params['initiator']['url'],params['request']['url'],params['requestId'])
          addReverseRedirToDict(reverseRedirPairs,params['initiator']['url'],params['request']['url'],params['requestId'])
    
    # Collecting responses received    
    elif(method == 'Network.responseReceived'):
      url = getUrl(params['response']['url'])
      if(url not in responses):
        responses[url] = set()
      responses[url].add(params['requestId'])
    
    # Meta, post and javascript redirects
    elif(method == 'Page.frameScheduledNavigation'):
      otherRedirects[getUrl(params['url'])] = params['reason']

  return (refererRedirPairs, redirPairs, responses, otherRedirects, httpRedirects, 
          reverseRefererRedirPairs, reverseRedirPairs, reverseHttpRedirects)
          

"""
Ads the next redirection hop to the redirection chain
"""
def addNextRedirHop(src,target,type,redirectionChain,responses,redirDict,i,nextSrc=None):
          
  redirectionChain.append(RedirectPair(src,target,type))
  
  if(nextSrc is not None):
    if(target in responses and 
      (type in ['http','frame','metaTagRefresh'] or 
      redirDict[nextSrc][i]['requestId'] in responses[target])):
      redirectionChain[-1].responseFound = True
    else:
      redirectionChain[-1].responseFound = False
    del redirDict[nextSrc][i]
    if(len(redirDict[nextSrc]) == 0):
      del redirDict[nextSrc]  
      
  else:
    if(target in responses and 
      (type in ['http','frame','metaTagRefresh'] or 
      redirDict[src][i]['requestId'] in responses[target])):
      redirectionChain[-1].responseFound = True
    else:
      redirectionChain[-1].responseFound = False
    del redirDict[src][i]
    if(len(redirDict[src]) == 0):
      del redirDict[src]  


"""
Ads the prev redirection hop to the reverse redirection chain
"""
def addPrevRedirHop(src,target,type,redirectionChain,responses,redirDict,i,nextTarget=None):
          
  redirectionChain.append(RedirectPair(src,target,type))
  
  if(nextTarget is not None):
    if(target in responses and 
      (type in ['http','frame','metaTagRefresh'] or 
      redirDict[nextTarget][i]['requestId'] in responses[target])):
      redirectionChain[-1].responseFound = True
    else:
      redirectionChain[-1].responseFound = False
    del redirDict[nextTarget][i]
    if(len(redirDict[nextTarget]) == 0):
      del redirDict[nextTarget]
      
  else:
    if(target in responses and 
      (type in ['http','frame','metaTagRefresh'] or 
      redirDict[target][i]['requestId'] in responses[target])):
      redirectionChain[-1].responseFound = True
    else:
      redirectionChain[-1].responseFound = False
    del redirDict[target][i]
    if(len(redirDict[target]) == 0):
      del redirDict[target]


"""
Checks if we can find one more redirection hop.
"""
def getNextRedirHop(src,nextSrc,redirPairs,otherRedirects,redirectionChain,responses,hop=0):
  
  if(hop > 10):
    return True
    
  # Check if we found a redirect
  for i in range(len(redirPairs[nextSrc])):
    target = redirPairs[nextSrc][i]['target']
    if(target in otherRedirects):
      addNextRedirHop(src,target,otherRedirects[target],redirectionChain,responses,redirPairs,i,nextSrc)
      return False

  # If we have not found a redirect see if the next hop can be a redirect   
  for i in range(len(redirPairs[nextSrc])):
    target = redirPairs[nextSrc][i]['target']
    if(target in redirPairs):
      redirectFound = not getNextRedirHop(src,target,redirPairs,otherRedirects,redirectionChain,responses,hop+1)
      if(redirectFound):
        del redirPairs[nextSrc][i]
        if(len(redirPairs[nextSrc]) == 0):
          del redirPairs[nextSrc]
        return False
  # If we have not found a redirect then it is the end of chain based on this and return True
  return True


"""
Checks if we can find one more redirection hop backwards.
"""
def getPrevRedirHop(target,nextTarget,reverseRedirPairs,otherRedirects,reverseChain,responses,hop=0):
  
  if(hop > 10):
    return True
    
  # Check if we found a redirect
  for i in range(len(reverseRedirPairs[nextTarget])):
    src = reverseRedirPairs[nextTarget][i]['src']
    if(target in otherRedirects):
      addPrevRedirHop(src,target,otherRedirects[target],reverseChain,responses,reverseRedirPairs,i,nextTarget)
      return False
  for i in range(len(reverseRedirPairs[nextTarget])):
    src = reverseRedirPairs[nextTarget][i]['src']
    addPrevRedirHop(src,target,'js',reverseChain,responses,reverseRedirPairs,i,nextTarget)
    return False
  # If we have not found a redirect see if the next hop can be a redirect   
  for i in range(len(reverseRedirPairs[nextTarget])):
    src = reverseRedirPairs[nextTarget][i]['src']
    if(src in reverseRedirPairs):
      redirectFound = not getPrevRedirHop(target,src,reverseRedirPairs,otherRedirects,reverseChain,responses,hop+1)
      if(redirectFound):
        del reverseRedirPairs[nextTarget][i]
        if(len(reverseRedirPairs[nextTarget]) == 0):
          del reverseRedirPairs[nextTarget]
        return False
  # If we have not found a redirect then it is the end of chain based on this and return True
  return True

  

def isMetaRedirect(src,metaUrls,entries,redirectionChain,responses):

  metaUrl = None
  minTime = 88888888888888888888888888
  index = -1
  redirectUrls = set([r.target for r in redirectionChain])
  for url in metaUrls:
    if(url in entries and url not in redirectUrls):
      for i in range(len(entries[url])):
        entry = entries[url][i]
        ts = float(time.mktime(datetime.strptime(entry['startedDateTime'][:-6],'%Y-%m-%dT%H:%M:%S.%f').timetuple()))
        if(ts < minTime):
          minTime = ts
          metaUrl = url
          index = i
  if(metaUrl is not None):
    redirectionChain.append(RedirectPair(src,metaUrl,'metaTagRefresh'))
    del entries[metaUrl][index]
    if(len(entries[metaUrl]) == 0):
      del entries[metaUrl]
    if(metaUrl in responses):
      redirectionChain[-1].responseFound = True
    else:
      redirectionChain[-1].responseFound = False
    return False
  else:
    return True
      
     
"""
This function reconstructs redirects from redirect pairs
"""
def reconstructRedirects(entries,redirDicts,startUrl,lastUrl, domContentDistribution):

  (refererRedirPairs, redirPairs, responses, otherRedirects, httpRedirects, 
  reverseRefererRedirPairs, reverseRedirPairs, reverseHttpRedirects) = redirDicts
  
  endOfChain = False
  metaUrls = set([x for x,y in otherRedirects.items() if y == 'metaTagRefresh'])
  metaDomains = {getDomain(x):x for x,y in otherRedirects.items() if y == 'metaTagRefresh'}
  redirectionChain = [RedirectPair(None,startUrl,'start')]
  if(startUrl in responses):
    redirectionChain[-1].responseFound = True
  else:
    redirectionChain[-1].responseFound = False  
    
  while(not endOfChain):
    nextSrc = redirectionChain[-1].target 
    if(lastUrl is not None and nextSrc == lastUrl):
      break
    endOfChain = True
    # Look for meta, javascript, form redirect
    if(nextSrc in refererRedirPairs):
      for i in range(len(refererRedirPairs[nextSrc])):
        target = refererRedirPairs[nextSrc][i]['target']
        if(target in otherRedirects):
          addNextRedirHop(nextSrc,target,otherRedirects[target],redirectionChain,responses,refererRedirPairs,i)
          endOfChain = False
          break
    # Look for meta, javascript, form redirect without referrer field set - more complex
    if(nextSrc in redirPairs and endOfChain):
      endOfChain = getNextRedirHop(nextSrc,nextSrc,redirPairs,otherRedirects,redirectionChain,responses)
    # HTTP redirects
    if(nextSrc in httpRedirects and endOfChain):
      for i in range(len(httpRedirects[nextSrc])):
        addNextRedirHop(nextSrc,httpRedirects[nextSrc][i]['target'],'http',redirectionChain,responses,httpRedirects,i)
        endOfChain = False
        break
    # Redirects not found in performance log -> using har file
    if(nextSrc in entries and endOfChain):
      for i in range(len(entries[nextSrc])):
        entry = entries[nextSrc][i]
        soup = getHtml(entry)
        # find base url for redirections
        baseUrl = str(nextSrc)
        if(soup is not None):
          base = soup.find('base')
          if((base is not None) and base.has_attr('href')):
            baseUrl = urljoin(baseUrl,base['href'])
          # 1. check for meta tag redirect
          metaRedirect = getMetaRedirect(entry,soup,baseUrl)
          metaDomain = getDomain(metaRedirect)
          if(metaRedirect is not None and metaDomain in metaDomains):
            newMetaRedirect = metaDomains[metaDomain]
            addNextRedirHop(nextSrc,newMetaRedirect,otherRedirects[newMetaRedirect],redirectionChain,responses,entries,i)
            endOfChain = False
            break
          # 2. frame content inclusion
          redirUrls = getTagRedirect(entry,soup,entries,baseUrl,['iframe', 'frame'],'src',nextSrc,domContentDistribution)
          if(len(redirUrls) > 0):
            for redirUrl in redirUrls:
              addNextRedirHop(nextSrc,redirUrl,'frame',redirectionChain,responses,entries,i)
              endOfChain = False
              break
        if(not endOfChain):
          break  
    if(len(redirectionChain) >= 64):
      endOfChain = True
          
    # Final effort to find meta redirect
    if(endOfChain and (lastUrl is None or nextSrc != lastUrl)):
      endOfChain = isMetaRedirect(nextSrc,metaUrls,entries,redirectionChain,responses)
  return redirectionChain
    
    
def getAdRedirDict(pair):

  return RedirectPair(pair['src'],pair['target'],'unknown')


def popFirstRedirPair(chains,redirDict,reverseRedirDict):

  pair = list(redirDict.values())[0][0]
  chains.append([getAdRedirDict(pair)])
  del redirDict[pair['src']][0]
  if(len(redirDict[pair['src']]) == 0):
    del redirDict[pair['src']]  

  return True
  
  
def addRedirHop(chains,redirPairs):

  target = chains[0][-1].target
  chains[0].append(getAdRedirDict(redirPairs[target][0]))
  
  if(len(redirPairs[target]) > 1):
    for i in range(len(redirPairs[target])-1):
      newChain = copy.deepcopy(chains[0][:-1])
      newChain.append(getAdRedirDict(redirPairs[target][i+1]))
      chains.append(newChain)
      
  # remove reverse
  del redirPairs[target]
  

def addReverseRedirHop(chains,reverseRedirPairs):

  src = chains[0][0].src
  chains[0].insert(0,getAdRedirDict(reverseRedirPairs[src][0]))
  
  if(len(reverseRedirPairs[src]) > 1):
    for i in range(len(reverseRedirPairs[src])-1):
      newChain = copy.deepcopy(chains[0][1:])
      newChain.insert(0,getAdRedirDict(reverseRedirPairs[src][i+1]))
      chains.append(newChain)
  # remove reverse
  del reverseRedirPairs[src]


def domLen(chain):

  return len(getDomainRedirectionChain(chain))


def getHtmlChains(finishedRedirectionChains,entries):

  htmlChains = []
  chainKeys = set()
  for chain in finishedRedirectionChains:
    lastHtmlIndex = -1
    for i in range(len(chain)):
      pair = chain[i]
      if(pair.target in entries):
        for entry in entries[pair.target]:
          if(isHtml(entry)):
            lastHtmlIndex = i
    newChain = chain[:lastHtmlIndex+1]
    if(len(newChain) > 2):
      doms = [x.target for x in getDomainRedirectionChain(newChain)]
      key = '-'.join(doms)
      if(len(doms) > 2 and key not in chainKeys):
        chainKeys.add(key)
        htmlChains.append(newChain)
      
  # return only the top 4 longest chains
  topChains = []
  curLen = 0
  for chain in sorted(htmlChains, key=domLen, reverse=True):
    l = domLen(chain)
    if(l != curLen):
      curLen = l
      if(len(topChains) > 3):
        break
    topChains.append(chain)
      
  return topChains


def getHarRedirectsForAds(entries,domContentDistribution):

  harRedirects = {}
  reverseHarRedirects = {}

  for src, entryList in entries.items():
    for entry in entryList:
      soup = getHtml(entry)
      # find base url for redirections
      baseUrl = str(src)
      if(soup is not None):
        base = soup.find('base')
        if((base is not None) and base.has_attr('href')):
          baseUrl = urljoin(baseUrl,base['href'])
        # 1. check for meta tag redirect
        metaRedirect = getMetaRedirect(entry,soup,baseUrl)
        if(metaRedirect is not None):
          if(src not in harRedirects):
            harRedirects[src] = []
          if(metaRedirect not in reverseHarRedirects):
            reverseHarRedirects[metaRedirect] = []
          harRedirects[src].append({'src':src,'target':metaRedirect})
          reverseHarRedirects[metaRedirect].append({'src':src,'target':metaRedirect})
        # 2. frame content inclusion
        redirUrls = getTagRedirect(entry,soup,entries,baseUrl,['iframe', 'frame'],'src',src,domContentDistribution)
        if(len(redirUrls) > 0):
          for redirUrl in redirUrls:
            if(src not in harRedirects):
              harRedirects[src] = []
            if(redirUrl not in reverseHarRedirects):
              reverseHarRedirects[redirUrl] = []
            harRedirects[src].append({'src':src,'target':redirUrl})
            reverseHarRedirects[redirUrl].append({'src':src,'target':redirUrl})
            
  return harRedirects, reverseHarRedirects
        

# Attempt to stich chains together in case they are found in otherRedirects
def stichChains(chains,otherRedirects):

  newChains = []
  for i in range(len(chains)):
    start = chains[i][0].src
    isFound = False
    if(start in otherRedirects):
      url = getUrl(str(start).split('?')[0])
      for j in range(len(chains)):
        end = getUrl(str(chains[j][-1].target).split('?')[0])
        if(url == end):
          isFound = True
          newChain = chains[j]
          newChain.append(RedirectPair(chains[j][-1].target,chains[i][0].src,'guessed'))
          newChain.extend(chains[i])
          newChains.append(newChain)
    if(not isFound):
      newChains.append(chains[i])
          
  return newChains
        
def perfectChains(chains,startUrl,noStartUrl):

  startDomain = getDomain(startUrl)
  newChains = []
  for chain in chains:
    if(len(chain) > 1):
      domain = getDomain(chain[0].src)
      newChain = copy.deepcopy(chain)
      if(not noStartUrl):
        if(domain != startDomain):
          newChain.insert(0,RedirectPair(startUrl,newChain[0].src,'adLink'))  
        newChain.insert(0,RedirectPair(None,newChain[0].src,'start'))
      newChains.append(newChain)  
  return newChains
    

def removeSubChains(chains,otherRedirects):

  filtered = [] 
  keys = set()
  for c1 in chains:
    k1 = '-'.join([str(x.target) for x in c1]).lower()
    for c2 in chains:
      k2 = '-'.join([str(x.target) for x in c2]).lower()
      if(k1 != k2 and k1 not in k2 and k1 not in keys 
        and 'recaptcha' not in k1 and 'areyouahuman' not in k1 and 'crwdcntrl.net' not in k1):
        filtered.append(c1)
        keys.add(k1)
  return filtered
    
    

def reconstructRedirectsAds(entries,redirDicts,domContentDistribution,startUrl, noStartUrl = False):

  finishedRedirectionChains = []
  chains = []
  
  (refererRedirPairs, redirPairs, responses, otherRedirects, httpRedirects, 
  reverseRefererRedirPairs, reverseRedirPairs, reverseHttpRedirects) = redirDicts
  harRedirects, reverseHarRedirects = getHarRedirectsForAds(entries,domContentDistribution)
   
  change = True
  while(change):
    change = False
    
    # First check if we can append to the extising redirection chain
    if(len(chains) > 0):
      endOfChain = False
      while(not endOfChain):
        endOfChain = True
        
        if(chains[0][-1].target in redirPairs):
          endOfChain = False
          addRedirHop(chains,redirPairs)
        elif(chains[0][0].src in reverseRedirPairs):
          endOfChain = False
          addReverseRedirHop(chains,reverseRedirPairs)
        elif(chains[0][-1].target in httpRedirects):
          endOfChain = False
          addRedirHop(chains,httpRedirects)
        elif(chains[0][0].src in reverseHttpRedirects):
          endOfChain = False
          addReverseRedirHop(chains,reverseHttpRedirects)
        elif(chains[0][-1].target in refererRedirPairs):
          endOfChain = False
          addRedirHop(chains,refererRedirPairs)
        elif(chains[0][0].src in reverseRefererRedirPairs):
          endOfChain = False
          addReverseRedirHop(chains,reverseRefererRedirPairs)
        elif(chains[0][-1].target in harRedirects):
          endOfChain = False
          addRedirHop(chains,harRedirects)
        elif(chains[0][0].src in reverseHarRedirects):
          endOfChain = False
          addReverseRedirHop(chains,reverseHarRedirects)
          
        if(len(chains[0]) > 20):
          endOfChain = True
          
      
      finishedRedirectionChains.append(chains[0])
      del chains[0]
      change = True
          
    # If we cannot then start a new redirection chain
    elif(len(redirPairs) != 0):
      change = popFirstRedirPair(chains,redirPairs,reverseRedirPairs)
    elif(len(httpRedirects) != 0):
      change = popFirstRedirPair(chains,httpRedirects,reverseHttpRedirects)
    elif(len(refererRedirPairs) != 0):
      change = popFirstRedirPair(chains,refererRedirPairs,reverseRefererRedirPairs)
    elif(len(harRedirects) != 0):
      change = popFirstRedirPair(chains,harRedirects,reverseHarRedirects)
      
  finishedRedirectionChains = stichChains(finishedRedirectionChains,otherRedirects)
  finishedRedirectionChains = perfectChains(finishedRedirectionChains,startUrl,noStartUrl)
  finishedRedirectionChains = removeSubChains(finishedRedirectionChains,otherRedirects)

  htmlChains = getHtmlChains(finishedRedirectionChains,entries)
  return htmlChains


"""
Get all the meta redirects from HAR and put them in a reverse dict
"""
def getReverseMetaRedirects(entries):

  reverseMetaRedirects = {}

  for url in entries:
    for i in range(len(entries[url])):
        entry = entries[url][i]
        soup = getHtml(entry)
        # find base url for redirections
        baseUrl = str(url)
        if(soup is not None):
          base = soup.find('base')
          if((base is not None) and base.has_attr('href')):
            baseUrl = urljoin(baseUrl,base['href'])
          # 1. check for meta tag redirect
          metaRedirect = getMetaRedirect(entry,soup,baseUrl)
          addReverseRedirToDict(reverseMetaRedirects,str(metaRedirect),str(url),'metaTagRefresh')
          
  return reverseMetaRedirects
  

def reconstructReverseRedirects(entries,redirDicts,startUrl,lastUrl,redirectionChain):

  (refererRedirPairs, redirPairs, responses, otherRedirects, httpRedirects, 
  reverseRefererRedirPairs, reverseRedirPairs, reverseHttpRedirects) = redirDicts
  reverseMetaRedirects = getReverseMetaRedirects(entries)


  middleUrl = redirectionChain[-1].target
  endOfChain = False
  reverseChain = [RedirectPair(lastUrl,None,'end')]
  if(lastUrl in responses):
    reverseChain[-1].responseFound = True
  else:
    reverseChain[-1].responseFound = False  
    
  while(not endOfChain):
    nextTarget = reverseChain[-1].src 
    if(nextTarget == middleUrl):
      break
    endOfChain = True
    # HTTP redirects
    if(nextTarget in reverseHttpRedirects and endOfChain):
      for i in range(len(reverseHttpRedirects[nextTarget])):
        addPrevRedirHop(reverseHttpRedirects[nextTarget][i]['src'],nextTarget,'http',reverseChain,responses,reverseHttpRedirects,i)
        endOfChain = False
        break
    # Look for meta, javascript, form redirect
    if(nextTarget in reverseRefererRedirPairs):
      for i in range(len(reverseRefererRedirPairs[nextTarget])):
        src = reverseRefererRedirPairs[nextTarget][i]['src']
        if(nextTarget in otherRedirects):
          addPrevRedirHop(src,nextTarget,otherRedirects[nextTarget],reverseChain,responses,reverseRefererRedirPairs,i)
          endOfChain = False
          break
    # Look for meta, javascript, form redirect without referrer field set - more complex
    if(nextTarget in reverseRedirPairs and endOfChain):
      endOfChain = getPrevRedirHop(nextTarget,nextTarget,reverseRedirPairs,otherRedirects,reverseChain,responses)
    # Redirects not found in performance log -> using har file
    if(nextTarget in reverseMetaRedirects and nextTarget in otherRedirects and endOfChain):
      addPrevRedirHop(reverseMetaRedirects[nextTarget][0]['src'],nextTarget,otherRedirects[nextTarget],reverseChain,responses,reverseMetaRedirects,0)
      endOfChain = False
    if(len(reverseChain) >= 64):
      endOfChain = True
      
  # Glueing redirection chains together
  reverseChain.reverse()
  if(reverseChain[0].src != redirectionChain[-1].target):
    redirectionChain.append(RedirectPair(redirectionChain[-1].target,reverseChain[0].src,'guessed'))
  redirectionChain.extend(reverseChain[:-1])
  
"""
Main function to get redirections
- guessing last URL - url with most content is not used/needed in this version anymore
"""
@timeout(128)
def getRedirectChain(performanceLog,har,startUrl,lastUrl=None):

  startUrl = getUrl(startUrl)
  startDomain = getDomain(startUrl)
  lastUrl = getUrl(lastUrl)
  # Load chrome performance log file
  redirDicts = processPerformanceLog(performanceLog,startUrl)
  # Load har file
  har_parser = HarParser(har)  
  entries = None
  for page in har_parser.pages:
    entries = getEntries(page)
  entriesSaved = deepcopy(entries)
  # Needed for certain redirections
  contentDistribution = {getUrl(x['request']['url']):x['response']['bodySize'] for x in page.entries}
  domContentDistribution = getPerDomainContent(contentDistribution)
  errorCodes = {getUrl(x['request']['url']):x['response']['status'] for x in page.entries}
  
  endOfChain = False
  redirectionChain = reconstructRedirects(entries,redirDicts,startUrl,lastUrl,domContentDistribution)
  if(lastUrl is not None and getDomain(redirectionChain[-1].target) != getDomain(lastUrl)):
    reconstructReverseRedirects(entries,redirDicts,startUrl,lastUrl,redirectionChain)
  counter = 0
  while(not endOfChain and counter < 4):
    counter += 1
    if(redirectionChain[-1].type == 'frame' or startUrl == lastUrl):
      redirDicts = processPerformanceLog(performanceLog,redirectionChain[-1].target)
      
      subChain = reconstructRedirects(entries,redirDicts,redirectionChain[-1].target,None,domContentDistribution)
      redirectionChain[-1].responseFound = subChain[0].responseFound
      if(len(subChain) == 1):
        endOfChain = True
      else:
        redirectionChain.extend(subChain[1:])
    else:
      endOfChain = True
      
  guessContentInclusion(redirectionChain,entriesSaved,contentDistribution,domContentDistribution)
  
  # Last attempt to fix some frame + meta redirect problems:
  if(redirectionChain[-1].type == 'frame'):
    redirDictsAds = processPerformanceLogAds(performanceLog)
    redirectionChains = reconstructRedirectsAds(entriesSaved,redirDictsAds,domContentDistribution,startUrl,noStartUrl = True)
    metaStarters = []  
    redirectionChains = sorted(redirectionChains, key=domLen, reverse=True)
    for chain in redirectionChains:
      domain = getDomain(chain[0].src)
      if(domain == startDomain and len(chain) > 1):
        redirectionChain.append(RedirectPair(redirectionChain[-1].target,chain[0].target,'guessed'))
        redirectionChain.extend(chain[1:])
        break

  return redirectionChain, contentDistribution, errorCodes, None  
  
  
"""
Main function to get redirections FROM ADS - only for URL shortening
- returns multiple redirection chains
- guessing last URL - url with most content is not used/needed in this version
"""
@timeout(128)
def getAdRedirectChains(performanceLog,har,startUrl):

  # Load chrome performance log file
  redirDicts = processPerformanceLogAds(performanceLog)
  # Load har file
  har_parser = HarParser(har)  
  entries = None
  for page in har_parser.pages:
    entries = getEntries(page)
  entriesSaved = deepcopy(entries)
  # Needed for certain redirections
  contentDistribution = {getUrl(x['request']['url']):x['response']['bodySize'] for x in page.entries}
  domContentDistribution = getPerDomainContent(contentDistribution)
  errorCodes = {getUrl(x['request']['url']):x['response']['status'] for x in page.entries}
  
  redirectionChains = reconstructRedirectsAds(entries,redirDicts,domContentDistribution,startUrl)
  
  return redirectionChains, contentDistribution, errorCodes, None
 

def getRedirectChainFromLandingUrlsHarOnly(har,url):

  (redirectPairs, inverseRedirectPairs, referers, 
  inverseReferers, contentDistribution, htmlUrls, 
  errorCodes, entries) = getRedirectSubChains(har)
  
  defragmentedUrl = getUrl(url)
  originalUrl = getUrl(url)
  if(url in entries):
    url = originalUrl
  else:
    url = defragmentedUrl
  
  reverseChain = buildReverseRedirChainUsingReferer(url,redirectPairs,inverseRedirectPairs,entries,referers,inverseReferers,4)
  redirectionChain = [RedirectPair(None,reverseChain[0].src,'start')]
  redirectionChain.extend(reverseChain[:-1])
  return redirectionChain


def getRedirectChainFromLandingUrlsHarOnly(har,urls):
    
  chains = {}
  for url in urls:
    chains[url] = getRedirectChainFromLandingUrlHarOnly(har,url)
  return chains
  
  
def getRedirectChainFromLandingUrls(har,windows):

  chains = {}
  for id, item in windows.items():
    performanceLog = loadFile(item['perflog'])
    startUrl = item['startUrl']
    landingUrl = item['landingUrl']
    redirectionChain, contentDistribution, errorCodes, g = getRedirectChain(performanceLog,har,startUrl,landingUrl)
    chains[id] = redirectionChain
  return chains


def printRedirs(redirectionChain):

  for redirectPair in redirectionChain: 
    print(' ',str(redirectPair.target),' - ',redirectPair.type)    
  print('\n')
  domRedirChain = getDomainRedirectionChain(redirectionChain)   
  for redirectPair in domRedirChain:
    print(' ',redirectPair.target,' - ',redirectPair.type)
  print('---------------------------------------------')
  print('---------------------------------------------\n\n')


def printReverseRedirs(redirectionChain):

  print('\n++++++++++++++++++++++++++++++++++++++++++++++++\n')
  for redirectPair in redirectionChain:
    print(' ',str(redirectPair.src)[:80],' - ',redirectPair.type)    
  print('\n')
  domRedirChain = getDomainRedirectionChain(redirectionChain)   
  for redirectPair in domRedirChain:
    print(' ',redirectPair.src,' - ',redirectPair.type)
  print('---------------------------------------------')
  print('---------------------------------------------\n\n')


