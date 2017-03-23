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

DB_MUTEX=threading.Lock()
SERVERLOG_MUTEX=threading.Lock()

TYPE_SET=set(['string','int','float'])

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




class ProjectOneHandler:
  def __init__(self):
    self.log = {}
 
  def PUT(self,key,key_type,value,value_type):
    key=convertType(key,key_type)
    value=convertType(value,value_type)
    addr=socket.gethostbyname(socket.gethostname())
    if DB_MUTEX.acquire():
      if haskey(key):
        db.set(key,value)
        db.dump()
        #default policy is to overwrite            
        consoleMSG=getTimestamp()+'  WARNING: Key '+str(key)+' has been overwritten. Requested by:'+str(addr)            
        consoleMSG_handler(consoleMSG)
        returnMSG="ServerWARNING: Key"+str(key)+' has been overwritten.'
        DB_MUTEX.release()
        return returnMSG
      else:
        db.set(key,value)
        db.dump()
        consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+' put successfully. Requested by:'+str(addr)
        consoleMSG_handler(consoleMSG)
        returnMSG="ServerINFO: Key"+str(key)+' put successfully.'
        DB_MUTEX.release()
        return returnMSG

 
  def GET(self,key,key_type):
    key=convertType(key,key_type)
    addr=socket.gethostbyname(socket.gethostname())
    if DB_MUTEX.acquire():
      if haskey(key):
        v=db.get(key)
        consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+' get successfully. Requested by:'+str(addr)
        consoleMSG_handler(consoleMSG)
        DB_MUTEX.release()
        return json.dumps({key:v})
      else:
        consoleMSG=getTimestamp()+'  ERROR: Key '+str(key)+' does not exist. Requested by:'+str(addr)
        consoleMSG_handler(consoleMSG)
        returnMSG="ServerERROR: Key"+str(key)+'does not exist.'
        DB_MUTEX.release()
        return returnMSG


  def DELETE(self,key,key_type):
    key=convertType(key,key_type)
    addr=socket.gethostbyname(socket.gethostname())    
    if DB_MUTEX.acquire():
      if haskey(key):
        db.rem(key)
        db.dump()
        consoleMSG=getTimestamp()+'  INFO: Key '+str(key)+'deleted succeddfully. Requested by:'+str(addr)
        consoleMSG_handler(consoleMSG)
        returnMSG="ServerINFO: Delete "+str(key)+'successfully.'
        DB_MUTEX.release()
        return returnMSG
      else:
        consoleMSG=getTimestamp()+'  WARNING: Key '+str(key)+' does not exist. Requested by:'+str(addr)
        consoleMSG_handler(consoleMSG)
        returnMSG="ServerWARNING: Key"+str(key)+'does not exist.'
        DB_MUTEX.release()
        return returnMSG


try:
  a,b=sys.argv[1].strip().split(':')
  #input validation
  assert(len(a.split('.'))==4)
  SERVER_IP=str(a)
  SERVER_PORT=int(b)
except Exception,e:
  print "Error: Input Syntax Error. Please check your Input."
  sys.exit(0) 

try:
  db=pk.load('KVStore2.db',False)
  consoleMSG=getTimestamp()+"  INFO:Database Loaded Successfully!"
  consoleMSG_handler(consoleMSG)
except Exception,e:
  consoleMSG=getTimestamp()+"  ERROR: Failed to load Databse!"
  consoleMSG_handler(consoleMSG)
  sys.exit(0)

handler = ProjectOneHandler()
processor = ProjectOne.Processor(handler)
transport = TSocket.TServerSocket(SERVER_IP,SERVER_PORT)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
 
server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

print "Starting python server..."
server.serve()
