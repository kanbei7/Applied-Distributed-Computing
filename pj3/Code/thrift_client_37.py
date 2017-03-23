#!/usr/bin/env python
import math
import sys
import os
import socket
import time
import threading
import pickledb as pk
import datetime
import json
import multiprocessing as mul
sys.path.append('./gen-py')
 
from ProjectOne import ProjectOne
from ProjectOne.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
CLIENTLOG_MUTEX=threading.Lock()

CMD_SET=set(['exit','PUT','DELETE','GET'])
TYPE_SET=set(['string','int','float'])
DEFAULT_TYPE='string'


def is_json(myjson):
	try:
		json.loads(myjson)
	except Exception:
		return False
	return True 


def getTimestamp():
	return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def checkTimeout(t):
	if int((time.time()-t)/60)>4:
		return True
	else:
		return False

def checkDataIntegrity(cmd,tmp):
	r1= (cmd=='PUT' and (len(tmp)<5 or tmp[4] not in TYPE_SET))
	r2= (cmd=='DELETE' or cmd == 'GET') and (len(tmp)<3)
	r3= tmp[2] not in TYPE_SET
	if cmd in CMD_SET:
		if r1 or r2 or r3:
			return True
		return False
	else:
		return True

def assignType(line):
	tmp=line.strip().split(',')
	N=len(tmp)
	if tmp=='exit':
		return 'exit'
	elif N==3:
		return ','.join([tmp[0],tmp[1],DEFAULT_TYPE,tmp[2],DEFAULT_TYPE])
	elif N==2:
		return ','.join([tmp[0],tmp[1]])
	else:
		return line

def writeClientLog(msg):
	if CLIENTLOG_MUTEX.acquire():
		f=open('clientLog_RPC.txt','a')
		f.writelines(msg+'\n')
		f.close()
		CLIENTLOG_MUTEX.release()

def consoleMSG_handler(msg):
	print (msg)
	writeClientLog(msg)

def convertType(s,t):
	if t=='string':
		return str(s)
	elif t=='int':
		return int(s)
	elif t=='float':
		return float(s)

def std(mylst):
	N=len(mylst)
	mean=sum(mylst)*1.0/N
	x=0.0
	for i in mylst:
		x+=(i-mean)*(i-mean)
	return math.sqrt(x/N)


SERVER_PORT=int(sys.argv[2])
SERVER_IP=['172.22.71.38','172.22.71.39','172.22.71.40','172.22.71.41','172.22.71.42']

try:
	INPUT=sys.argv[1]
	f=open(INPUT,'r')
	operations=f.readlines()
	f.close()
except Exception,e:
	consoleMSG=getTimestamp()+' ERROR: Can NOT open file. Program terminated.'
	consoleMSG_handler(consoleMSG)
	sys.exit(0)


def worker(lines):
	ops_sum=0
	time_recorder=[]
	server_pnt=0
	for line in lines:
		tmp=line.split(',')
		cmd=tmp[0]
	key=tmp[1]
	transport = TSocket.TSocket(SERVER_IP[server_pnt], SERVER_PORT)
	transport = TTransport.TBufferedTransport(transport)
	protocol = TBinaryProtocol.TBinaryProtocol(transport)
	client = ProjectOne.Client(protocol)
	FUNC_DICT={'PUT':client.PUT,'GET':client.GET,'DELETE':client.DELETE}
	transport.open()
	if cmd not in CMD_SET:
		continue
	elif cmd=='PUT':
		t1=time.time()
		value=tmp[2]
		result=FUNC_DICT[cmd](key,DEFAULT_TYPE,value,DEFAULT_TYPE)
	else:
		t1=time.time()
		result=FUNC_DICT[cmd](key,DEFAULT_TYPE)
	while True:
		if result!=None:
			flag=0
			break
		elif not len(result) and checkTimeout(t1):
			flag=1
			break
	if flag:
		consoleMSG=getTimestamp()+'WARNING: Operation timed Out. Server no response.'
		consoleMSG_handler(consoleMSG) 
		continue
	t2=time.time()
	delta=t2-t1
	time_recorder.append(delta)
	consoleMSG=getTimestamp()+'	'+str(result)
	consoleMSG_handler(consoleMSG)
	server_pnt+=1
	server_pnt%=5
	transport.close()
	ops_sum=len(time_recorder)
	print "Number of valid Operation:"
	print str(ops_sum)
	print "Average Time per ops"
	print str(sum(time_recorder)/ops_sum)
	print "STD of average time per ops"
	print str(std(time_recorder))
	print "Average TP"
	print str(ops_sum/sum(time_recorder))

p1 = mul.Process(target=worker, args=(operations))
p2 = mul.Process(target=worker, args=(operations))
p1.start()
p2.start()
p1.join()
p2.join()
