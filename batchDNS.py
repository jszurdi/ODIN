import dns
import dns.resolver
import itertools
from multiprocessing import Manager
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import Lock
import time


"""
Query dns for (domain, qname) and return (domain, qname, record)
"""
def oneDnsQuery(arg):

  
  resolver = dns.resolver.Resolver()
  resolver.timeout = 10
  try:
    domain, qname = arg
    rdatalist = [rdata.to_text() for rdata in resolver.query(domain, qname)]
    return domain, qname, rdatalist
  except dns.exception.DNSException as e:
    return domain, qname, []
    
    
def addRecord(responseDict,domain,type,results):
    
  # Save only if we found results
  if(len(results) > 0):
    if(domain not in responseDict):
      responseDict[domain] = {}
    if(type not in responseDict[domain]):
      responseDict[domain][type] = set()
    for result in results:
      responseDict[domain][type].add(result)
    return 1
  else:
    return 0
    
    
def worker(workQueue, responseList, successDict, lock):

  success = 0
  total = 0
  responseDict = {}
  while True:
    job = workQueue.get()
    if(len(job) == 1 and job[0] == "DONE"):
      break
    total += 1
    domain, qname, rdatalist = oneDnsQuery(job)
    key = qname.lower()+'s'
    success += addRecord(responseDict,domain,key,rdatalist)
  lock.acquire()
  responseList.append(responseDict)
  successDict['success'] += success
  successDict['failure'] += (total - success)
  lock.release()
   
  
"""
Given a list of hosts, return dict that maps qname to
returned rdata records.
"""      
def resolveDns(domains,recordTypes,numThreads = 4):

  print('DNS query intensity (number of processes used): ',numThreads)

  resolveStart = time.time()
  threads = []
  workQueue = Queue()
  manager = Manager()
  responseList = manager.list()
  successDict = manager.dict()
  lock = Lock()
  successDict['success'] = 0
  successDict['failure'] = 0
  for job in itertools.product(domains, recordTypes):
    workQueue.put(job)
  print("Number of queries initiated: ",  workQueue.qsize())
  for i in range(numThreads):
    workQueue.put(["DONE"])
  # Start workers
  for i in range(numThreads):
      p = Process(target = worker, args = (workQueue, responseList, successDict, lock))
      p.start()
      threads.append(p)
  # Join threads
  for t in threads:
    t.join()
  
  responseDict = {}
  # Sum the results:
  for rDict in responseList:
    for domain, records in rDict.items():
      for type, results in records.items(): 
        addRecord(responseDict,domain,type,results)

  print("Took [" + str(time.time() - resolveStart) + "] seconds to resolve: ",successDict['success'],' domain names.')
  print("Number of domains not resolved: ", successDict['failure'])
  return responseDict

  
if __name__ ==  '__main__':

  # domains = ['example.com', 'stackoverflow.com','google.com', 'thrashermagazine.com'] * 10000
  domains = ['example.balal', 'balablalk3l2kl.com','google.com', '2qdqdqd23qd4ttl2kt2l.co.uk'] * 10000
  result = resolveDns(domains, ('A', 'NS'))
  for domain, records in result.items():
    for type, results in records.items():
      print(domain, ' ', type)
      print(results)