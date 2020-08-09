import json
import base64
import zlib
import os
import typing  # noqa
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import time
import pickle
import sys
from urllib.parse import urlparse, parse_qs, unquote_plus

import mitmproxy
from mitmproxy import http
from mitmproxy import ctx
from mitmproxy.net.http.headers import Headers
from mitmproxy import connections  # noqa
from mitmproxy import version
from mitmproxy import ctx
from mitmproxy.utils import strutils
from mitmproxy.net.http import cookies

from redirectChainExtractor import getUrl


HAR: typing.Dict = {}
URL: typing.Dict = {}
HEADERS_TO_CHANGE: typing.Dict = {}
COMMANDS: typing.Dict = {}
# A list of server seen till now is maintained so we can avoid
# using 'connect' time for entries that use an existing connection.
SERVERS_SEEN: typing.Set[connections.ServerConnection] = set()


def harCollectionSetup(mainUrl):

  URL.update({'url':mainUrl})
  HAR.update({
    "log": {
      "version": "1.2",
      "creator": {
        "name": "mitmproxy har_dump",
        "version": "0.1",
        "comment": "mitmproxy version %s" % version.MITMPROXY
      },
      "pages": [{
        "id": mainUrl, 
        "startedDateTime": datetime.now().replace(tzinfo=timezone(offset=utcOffset())).isoformat(), 
        "title": mainUrl, 
        "pageTimings": {"comment": ""}, "comment": ""}],
      "entries": []
      }})

  
def request(flow):

  global HAR
  global URL
  global HEADERS_TO_CHANGE
  parsedUrl = urlparse(flow.request.url)
  hostname = parsedUrl.hostname
  params = parse_qs(parsedUrl.query)
  # ctx.log.error(hostname)
  if(hostname == 'startharcollection'):
    HAR = {}
    URL = {}
    harCollectionSetup(unquote_plus(params['url'][0]))
    resp = http.HTTPResponse.make(200,  b"true",{"Content-Type": "text/html"})
    flow.response = resp
  elif(hostname == 'gethar'):
    hardump = pickle.dumps(HAR)
    resp = http.HTTPResponse.make(200, hardump, {"Content-Type": "text/html"})
    flow.response = resp
    HAR = {}
    URL = {}
  elif(hostname == 'addheader'):
    header = next(iter(params))
    value = unquote_plus(params[header][0])
    header = unquote_plus(header)
    HEADERS_TO_CHANGE[header] = value
    resp = http.HTTPResponse.make(200,  b"true",{"Content-Type": "text/html"})
    flow.response = resp
  elif(hostname == 'removeaddheader'):
    header = unquote_plus(next(iter(params)))
    if(header in HEADERS_TO_CHANGE):
      del HEADERS_TO_CHANGE[header]
    resp = http.HTTPResponse.make(200,  b"true",{"Content-Type": "text/html"})
    flow.response = resp
  elif(hostname == 'removeresponse'):
    COMMANDS['removeResponse'] = True
    resp = http.HTTPResponse.make(200,  b"true",{"Content-Type": "text/html"})
    flow.response = resp
  elif(hostname == 'keepresponse'):
    COMMANDS['removeResponse'] = False
    resp = http.HTTPResponse.make(200,  b"true",{"Content-Type": "text/html"})
    flow.response = resp
  # If request is not one of our commands and we already set the url property
  # flow.request.method == "GET"
  elif('removeResponse' in COMMANDS and COMMANDS['removeResponse'] 
       and flow.request.method == "GET"):
    resp = http.HTTPResponse.make(200,b'<html><body>Modified for log collection</body></html>',{"Content-Type": "text/html"})
    flow.response = resp
    flow.response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    flow.response.headers["Pragma"] = "no-cache"
    flow.response.headers["Expires"] = "0"
  elif('url' in URL and getUrl(flow.request.url) == getUrl(URL['url'])):
    for header, value in HEADERS_TO_CHANGE.items():
      if(header not in flow.request.headers):
        flow.request.headers[header] = value

    
def utcOffset():

  utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
  utc_offset = timedelta(seconds=-utc_offset_sec)
  return utc_offset


def response(flow):
  """
  Called when a server response has been received.
  """
  hostname = urlparse(flow.request.url).hostname
  if(hostname in ['startharcollection','gethar','addheader','removeaddheader',
                  'removeresponse','keepresponse']):
    return
  if('url' not in URL or URL['url'] == ''):
    return
  if('removeResponse' in COMMANDS and COMMANDS['removeResponse']
    and flow.request.method == "GET"):
    return
    
  flow.response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
  flow.response.headers["Pragma"] = "no-cache"
  flow.response.headers["Expires"] = "0"
    
  # -1 indicates that these values do not apply to current request
  ssl_time = -1/1000
  connect_time = -1/1000

  try:
    if(flow.server_conn and flow.server_conn not in SERVERS_SEEN):
      connect_time = (flow.server_conn.timestamp_tcp_setup -
                      flow.server_conn.timestamp_start)

      if(flow.server_conn.timestamp_tls_setup is not None):
        ssl_time = (flow.server_conn.timestamp_tls_setup -
                    flow.server_conn.timestamp_tcp_setup)

      SERVERS_SEEN.add(flow.server_conn)
    # Calculate raw timings from timestamps. DNS timings can not be calculated
    # for lack of a way to measure it. The same goes for HAR blocked.
    # mitmproxy will open a server connection as soon as it receives the host
    # and port from the client connection. So, the time spent waiting is actually
    # spent waiting between request.timestamp_end and response.timestamp_start
    # thus it correlates to HAR wait instead.
    timings_raw = {
      'send': flow.request.timestamp_end - flow.request.timestamp_start,
      'receive': flow.response.timestamp_end - flow.response.timestamp_start,
      'wait': flow.response.timestamp_start - flow.request.timestamp_end,
      'connect': connect_time,
      'ssl': ssl_time,
    }

    # HAR timings are integers in ms, so we re-encode the raw timings to that format.
    timings = dict([(k, int(1000 * v)) for k, v in timings_raw.items()])

    # full_time is the sum of all timings.
    # Timings set to -1 will be ignored as per spec.
    full_time = sum(v for v in timings.values() if v > -1)

    started_date_time = datetime.fromtimestamp(flow.request.timestamp_start).replace(tzinfo=timezone(offset=utcOffset())).isoformat()

    # Response body size and encoding
    response_body_size = len(flow.response.raw_content)
    response_body_decoded_size = len(flow.response.content)
    response_body_compression = response_body_decoded_size - response_body_size

    entry = {
      "pageref":URL['url'],
      "startedDateTime": started_date_time,
      "time": full_time,
      "request": {
        "method": flow.request.method,
        "url": flow.request.url,
        "httpVersion": flow.request.http_version,
        "cookies": format_request_cookies(flow.request.cookies.fields),
        "headers": name_value(flow.request.headers),
        "queryString": name_value(flow.request.query or {}),
        "headersSize": len(str(flow.request.headers)),
        "bodySize": len(flow.request.content),
      },
      "response": {
        "status": flow.response.status_code,
        "statusText": flow.response.reason,
        "httpVersion": flow.response.http_version,
        "cookies": format_response_cookies(flow.response.cookies.fields),
        "headers": name_value(flow.response.headers),
        "content": {
          "size": response_body_size,
          "compression": response_body_compression,
          "mimeType": flow.response.headers.get('Content-Type', '')
        },
        "redirectURL": flow.response.headers.get('Location', ''),
        "headersSize": len(str(flow.response.headers)),
        "bodySize": response_body_size,
      },
      "cache": {},
      "timings": timings,
    }

    # Store binary data as base64
    if(strutils.is_mostly_bin(flow.response.content)):
      entry["response"]["content"]["text"] = base64.b64encode(flow.response.content).decode()
      entry["response"]["content"]["encoding"] = "base64"
    else:
      entry["response"]["content"]["text"] = flow.response.get_text(strict=False)

    if(flow.request.method in ["POST", "PUT", "PATCH"]):
      params = [
        {"name": a, "value": b}
        for a, b in flow.request.urlencoded_form.items(multi=True)
      ]
      entry["request"]["postData"] = {
        "mimeType": flow.request.headers.get("Content-Type", ""),
        "text": flow.request.get_text(strict=False),
        "params": params
      }

    if(flow.server_conn.connected()):
      entry["serverIPAddress"] = str(flow.server_conn.ip_address[0])
      
  except Exception as e:
    print('Mitmproxy, adding entry to HAR failed: ', e)
  else:
    HAR["log"]["entries"].append(entry)


def format_cookies(cookie_list):

  rv = []
  for name, value, attrs in cookie_list:
    cookie_har = {
      "name": name,
      "value": value,
    }
    # HAR only needs some attributes
    for key in ["path", "domain", "comment"]:
      if key in attrs:
        cookie_har[key] = attrs[key]
    # These keys need to be boolean!
    for key in ["httpOnly", "secure"]:
      cookie_har[key] = bool(key in attrs)
    # Expiration time needs to be formatted
    expire_ts = cookies.get_expiration_ts(attrs)
    if expire_ts is not None:
      cookie_har["expires"] = datetime.fromtimestamp(expire_ts).replace(tzinfo=timezone(offset=utcOffset())).isoformat()
    rv.append(cookie_har)
  return rv


def format_request_cookies(fields):

  return format_cookies(cookies.group_cookies(fields))


def format_response_cookies(fields):

  return format_cookies((c[0], c[1][0], c[1][1]) for c in fields)


def name_value(obj):
  """
  Convert (key, value) pairs to HAR format.
  """
  return [{"name": k, "value": v} for k, v in obj.items()]


