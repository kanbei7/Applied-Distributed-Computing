import sys
sys.path.append('./gen-py')
 
from ProjectOne import ProjectOne
from ProjectOne.ttypes import *
 
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
import os
import socket
import time
import threading
import pickledb as pk
import datetime
import json
import multiprocessing as mp

#SERVER_IP=['172.22.71.42','172.22.71.38','172.22.71.40','172.22.71.41']
#SERVER_IP=['172.22.71.42','172.22.71.39','172.22.71.40','172.22.71.41']
#SERVER_IP=['172.22.71.42','172.22.71.38','172.22.71.39','172.22.71.41']
#SERVER_IP=['172.22.71.42','172.22.71.38','172.22.71.40','172.22.71.39']
SERVER_IP=['172.22.71.41','172.22.71.38','172.22.71.40','172.22.71.39']

DB_MUTEX=threading.Lock()
SERVERLOG_MUTEX=threading.Lock()
TYPE_SET=set(['string','int','float'])
DEFAULT_TYPE='string'

def getTimestamp():
  return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def writeServerLog(msg):
  if SERVERLOG_MUTEX.acquire():
    f=open('serverLog_RPC.txt','a')
    f.writelines(msg+'\n')
    f.close()
    SERVERLOG_MUTEX.release()
def convertType(s,t):
  if t=='string':
    return str(s)
  elif t=='int':
    return int(s)
  elif t=='float':
    return float(s)

def consoleMSG_handler(msg):
  print (msg)
  writeServerLog(msg)

def haskey(key):
  if key in set(db.getall()):
    return True
  else:
    return False

def PUT_LOCAL1(key,key_type,value,value_type):
  key=convertType(key,key_type)
  value=convertType(value,value_type)
  if DB_MUTEX.acquire():
    if haskey(key):
      db.set(key,value)
      db.dump()
        #default policy is to overwrite            
      consoleMSG=getTimestamp()+'  WARNING: Key '+str(key)+' has been overwritten. '          
      consoleMSG_handler(consoleMSG)
      returnMSG="ServerWARNING: Key"+str(key)+' has been overwritten.'
      DB_MUTEX.release()       
    else:
      db.set(key,value)
      db.dump()
      consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+' put successfully.'
      consoleMSG_handler(consoleMSG)
      returnMSG="ServerINFO: Key"+str(key)+' put successfully.'
      DB_MUTEX.release()
    return returnMSG

def DELETE_LOCAL1(key,key_type):
  key=convertType(key,key_type)
  if DB_MUTEX.acquire():
    if haskey(key):
      db.rem(key)
      db.dump()
      consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+'deleted successfully.'
      consoleMSG_handler(consoleMSG)
      returnMSG="ServerINFO: Delete "+str(key)+'successfully.'
      DB_MUTEX.release()
    else:
      consoleMSG=getTimestamp()+'  WARNING: Key '+str(key)+' does not exist. '
      consoleMSG_handler(consoleMSG)
      returnMSG="ServerWARNING: Key"+str(key)+'does not exist.'
      DB_MUTEX.release()
    return returnMSG

def PUT_SYNC_WORKER(ip, port,key,key_type,value,value_type):
  transport = TSocket.TSocket(ip, port)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ProjectOne.Client(protocol)
  transport.open()
  result=client.PUT_LOCAL(key,DEFAULT_TYPE,value,DEFAULT_TYPE)
  transport.close()
  if type(result)!=type('string'):
    return 1
  else:
    return 0

def PUT_SYNC(key,key_type,value,value_type):
  for ip in SERVER_IP:
    x=PUT_SYNC_WORKER(ip,SERVER_PORT,key,key_type,value,value_type)
  return True

def DEL_SYNC_WORKER(ip, port,key,key_type):
  transport = TSocket.TSocket(ip, port)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
  client = ProjectOne.Client(protocol)
  transport.open()
  result=client.DELETE_LOCAL(key,DEFAULT_TYPE)
  transport.close()
  if type(result)!=type('string'):
    return 1
  else:
    return 0

def DEL_SYNC(key,key_type):
  for ip in SERVER_IP:
    x=DEL_SYNC_WORKER(ip,SERVER_PORT,key,key_type)
  return True


class ProjectOneHandler:
  def __init__(self):
    self.log = {}
  def PUT_LOCAL(self,key,key_type,value,value_type):
    return PUT_LOCAL1(key,key_type,value,value_type)
  def DELETE_LOCAL(self,key,key_type):
    return DELETE_LOCAL1(key,key_type)
  def PUT(self,key,key_type,value,value_type):  
    x=PUT_SYNC(key,key_type,value,value_type)
    consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+'Put Synchronization successfully. '
    consoleMSG_handler(consoleMSG)
    return PUT_LOCAL1(key,key_type,value,value_type)
  def GET(self,key,key_type):
    key=convertType(key,key_type)
    if DB_MUTEX.acquire():
      if haskey(key):
        v=db.get(key)
        consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+' get successfully. '
        consoleMSG_handler(consoleMSG)
        DB_MUTEX.release()
        return json.dumps({key:v})
      else:
        consoleMSG=getTimestamp()+'  ERROR: Key '+str(key)+' does not exist. '
        consoleMSG_handler(consoleMSG)
        returnMSG="ServerERROR: Key"+str(key)+'does not exist.'
        DB_MUTEX.release()
        return returnMSG
  def DELETE(self,key,key_type):
    x=DEL_SYNC(key,key_type)
    consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+'Delete Synchronization successfully. '
    consoleMSG_handler(consoleMSG)
    return DELETE_LOCAL1(key,key_type)

try:
  a,b=sys.argv[1].strip().split(':')
  #input validation
  assert(len(a.split('.'))==4)
  HOST_IP=str(a)
  SERVER_PORT=int(b)
except Exception,e:
  print "Error: Input Syntax Error. Please check your Input."
  sys.exit(0) 

try:
  db=pk.load('kvstore_proj2.db',False)
  consoleMSG=getTimestamp()+"  INFO:Database Loaded Successfully!"
  consoleMSG_handler(consoleMSG)
except Exception,e:
  consoleMSG=getTimestamp()+"  ERROR: Failed to load Databse!"
  consoleMSG_handler(consoleMSG)
  sys.exit(0)

handler = ProjectOneHandler()
processor = ProjectOne.Processor(handler)
transport1 = TSocket.TServerSocket(HOST_IP,SERVER_PORT)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
 
server = TServer.TSimpleServer(processor, transport1, tfactory, pfactory)

print "Starting python server..."
server.serve()
