import requests
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
from urllib.parse import urljoin, urlparse, parse_qsl, unquote_plus
import random
import re
import sys

from linkFollow import LinkFollow
import redirectChainExtractor as rce
from config import Config

def getMovieTitles():

  movieTitles = set()
  headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0'}
  years = ['2019','2018','2017','2016','2015']
  # Movie titles
  for year in years:
    url = 'https://www.boxofficemojo.com/yearly/chart/?view=releasedate&view2=domestic&yr='+year+'&p=.htm'
    page = requests.get(url,headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')
    links = soup.find_all('a')
    for link in links:
      if(link.has_attr('href') and link['href'].startswith('/movies/?id=')):
        title = re.sub('[^0-9a-zA-Z]+', ' ', link.text.lower())
        title = ' '.join([x for x in title.split(' ') if len(x) > 3])
        if(len(title) > 5):
          movieTitles.add(title)
  # Load TV series titles
  seriesTitles = set()
  url = 'https://www.imdb.com/search/title?title_type=tv_series&view=simple&sort=moviemeter,asc&start=1&count=200'
  page = requests.get(url,headers=headers)
  soup = BeautifulSoup(page.text, 'html.parser')
  titleDivs = soup.find_all('div', {'class': 'lister-col-wrapper'})
  for titleDiv in titleDivs:
    link = titleDiv.find('a')
    title = re.sub('[^0-9a-zA-Z]+', ' ', link.text.lower())
    title = ' '.join([x for x in title.split(' ') if len(x) > 3])
    if(len(title) > 5 and title not in movieTitles):
      seriesTitles.add(title)
  print('Number of movie titles loaded: ',len(movieTitles))
  print('Number of series titles loaded: ',len(seriesTitles))
  return [x.split(' ') for x in movieTitles], [x.split(' ') for x in seriesTitles]
  

def getBaseUrl(url,soup):

  baseUrl = url
  if(soup is not None):
    base = soup.find('base')
    if((base is not None) and base.has_attr('href')):
      baseUrl = urljoin(baseUrl,base['href'])
  return baseUrl
  
  
def isUrlValid(url):

  regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
  return re.match(regex, url) is not None 

  
def findTitleInPart(part, titles):

  if(len(part) < 6):
    return False
  for title in titles:
    movieness = 0
    for word in title:
      if(part.find(word) != -1):
        movieness += 1
        if(len(title) == 1 or movieness == 2):
          return True
  return False
  
  
def isMovieUrl(url,titles):

  parts = urlparse(url)
  queries = parse_qsl(parts.query)
  path = unquote_plus(parts.path)
  if(len(path) > 0  and path[-1] == '/'):
    path = path[:-1]
  pathEnd = path.split('/')[-1]
  for query in queries:
    part = unquote_plus(query[1])
    if(findTitleInPart(part.lower(),titles)):
      return True
  if(findTitleInPart(pathEnd.lower(),titles)):
    return True
  return False
      
  
def findIllegalStreams(movieTitles, seriesTitles):

  sampleSize = 4
  finalUrls = {}
  url = 'https://en.softonic.com/solutions/what-are-the-best-free-movie-streaming-sites-without-sign-up'
  # now redirects to: https://binge.co/what-are-the-best-free-movie-streaming-sites-without-sign-up
  headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0'}

  runConfig = Config('runConfig.txt')
  display = Display(visible = 0, size = (1280, 768))
  display.start()
  scrapeType = {'name':'referrer','ua':'uaNormal','ref':True,'browser':'chrome','mobile':False}

  streamingSites = set()
  page = requests.get(url,headers=headers)
  soup = BeautifulSoup(page.text, 'html.parser')
  links = soup.findAll('a', {'class': 'card-solution__title'})
  for link in links:
    try:
      subpage = requests.get(link['href'],headers=headers)
    except Exception as e:
      print('Error in loading streaming site: ', e)
      continue
    subsoup = BeautifulSoup(subpage.text, 'html.parser')
    
    streamingLink = subsoup.find('a', {'class': 'js-get-solution'})
    if(streamingLink is not None and streamingLink['href'] != ''):
      streamingSite = streamingLink['href']
      streamingSites.add(streamingSite)
  print("Number of streaming sites: ",len(streamingSites)) 

  for streamingSite in streamingSites:
    print('Streaming site: ',streamingSite)
    if(not streamingSite.startswith('http')):
      streamingSite = 'http://'+streamingSite
    streamingDomain = rce.getDomain(streamingSite)
    follower = LinkFollow(1, runConfig, scrapeType, None)
    result = follower.followLink(streamingSite)
    try:
      follower.driver.quit()
    except:
      pass
    follower = None
    if(result is None):
      continue
    streamingSoup = BeautifulSoup(result[3], 'html.parser')
    baseUrl = getBaseUrl(streamingSite,streamingSoup)
    videoLinks = streamingSoup.findAll('a')
    movieUrls = set()
    seriesUrls = set()
    for videoLink in videoLinks:
      if(videoLink.has_attr('href')):
        videoUrl = urljoin(baseUrl,videoLink['href'])
        videoDomain = rce.getDomain(videoUrl)
        if(isUrlValid(videoUrl) and videoDomain == streamingDomain):
          if(isMovieUrl(videoUrl,movieTitles)):
            movieUrls.add(videoUrl)
          if(isMovieUrl(videoUrl,seriesTitles)):
            seriesUrls.add(videoUrl)
          
    sampleUrls = random.sample(list(movieUrls),min(len(movieUrls),sampleSize))
    if(len(sampleUrls) < sampleSize):
      sampleUrls += random.sample(list(seriesUrls),min(len(seriesUrls),sampleSize-len(sampleUrls)))
    
    for sampleUrl in sampleUrls:
      finalUrls[sampleUrl] = {'source':url}
    print('Number of copyright infringing URls selected: ',len(finalUrls))
  display.stop()
  return finalUrls


if __name__ == '__main__':

  movieTitles, seriesTitles = getMovieTitles()
  finalUrls = findIllegalStreams(movieTitles, seriesTitles)
