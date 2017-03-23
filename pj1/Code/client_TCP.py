#! /usr/bin/python
import os
import sys
import socket
import time
import threading
import datetime
import json
import math

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
        f=open('clientLog_TCP.txt','a')
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

if __name__ == "__main__":
	ops_sum=0
	time_recorder=[]
	try:
		a,b=sys.argv[1].strip().split(':')
        #input validation
		assert(len(a.split('.'))==4)
		SERVER_IP=str(a)
		SERVER_PORT=int(b)
		addr=(SERVER_IP,SERVER_PORT)
	except Exception,e:
		print "Error: Input Syntax Error. Please check your Input."
		sys.exit(0)
	try:
		INPUT=sys.argv[2]
		f=open(INPUT,'r')
		lines=f.readlines()
		f.close()
	except Exception,e:
		consoleMSG=getTimestamp()+'  ERROR: Can NOT open file. Program terminated.'
		consoleMSG_handler(consoleMSG)
		sys.exit(0)
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try: 
		s.connect(addr)
	except Exception,e:
		consoleMSG=getTimestamp()+'  ERROR: Can NOT Estblish connection. Program terminated.'
		consoleMSG_handler(consoleMSG_handler)
		consoleMSG_handler(e)
		sys.exit(0)

	#========Send OPS one by one===========
	for line in lines:
		msg=assignType(line)
		t1=time.time()
		s.send(msg)
		#=======checked if timed out========
		while True:
			result=s.recv(1024)
			if result!=None:
				flag=0
				break
			elif not len(result) and checkTimeout(t1):
				flag=1
				break

		if flag:
			consoleMSG=getTimestamp()+'  WARNING: Operation timed Out. Server no response.'
			consoleMSG_handler(consoleMSG)
			continue

		t2=time.time()
		delta=t2-t1
		time_recorder.append(delta)
		consoleMSG=getTimestamp()+'  '+result
		consoleMSG_handler(consoleMSG)
	s.send('exit')
	ops_sum=len(time_recorder)
	print "Number of valid Operation:"
	print str(ops_sum)
	print "Average Time per ops"
	print str(sum(time_recorder)/ops_sum)
	print "STD of average time per ops"
	print str(std(time_recorder))
	print "Average TP"
	print str(ops_sum/sum(time_recorder))








