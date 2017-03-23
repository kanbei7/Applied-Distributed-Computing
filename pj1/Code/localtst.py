import os
import sys
import socket
import time
import threading
import datetime
import json
import math
import pickledb as pk

ops_sum=0
time_recorder=[]
def std(mylst):
    N=len(mylst)
    mean=sum(mylst)*1.0/N
    x=0.0
    for i in mylst:
        x+=(i-mean)*(i-mean)
    return math.sqrt(x/N)

def haskey(key):
    if key in set(db.getall()):
        return True
    else:
        return False

def get(k):
	if haskey(k):
		return db.get(k)
	else:
		return 'KeyError'

def delete(k):
	if haskey(k):
		return db.rem(k)
	else:
		return 'KeyError'

def put(k,v):
	return db.set(k,v)

db=pk.load('KVlocal.db',False)


INPUT=sys.argv[1]
f=open(INPUT,'r')
lines=f.readlines()
f.close()
for line in lines:
	t1=time.time()
	tmp=line.strip().split(',')
	cmd=tmp[0]
	key=tmp[1]
	if cmd=='PUT':
		x=put(key,tmp[2])
	elif cmd=='GET':
		x=get(key)
	elif cmd=='DELETE':
		x=delete(key)
	t2=time.time()
	time_recorder.append(t2-t1)


ops_sum=len(time_recorder)
print "Number of valid Operation:"
print str(ops_sum)
print "Average Time per ops"
print str(sum(time_recorder)/ops_sum)
print "STD of average time per ops"
print str(std(time_recorder))
print "Average TP"
print str(ops_sum/sum(time_recorder))