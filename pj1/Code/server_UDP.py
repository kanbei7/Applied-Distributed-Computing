#! /usr/bin/python
import os
import sys
import socket
import time
import threading
import pickledb as pk
import datetime
import json


BLK_MUTEX=threading.Lock()
DB_MUTEX=threading.Lock()
SERVERLOG_MUTEX=threading.Lock()

CMD_SET=set(['PUT','DELETE','GET'])
TYPE_SET=set(['string','int','float'])

def is_json(myjson):  
    try:  
        json.loads(myjson)  
    except Exception:  
        return False  
    return True 

def getTimestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def checkTimeout(t):
    if int((time.time()-t)/60)>20:
        return True
    else:
        return False

def convertType(s,t):
    if t=='string':
        return str(s)
    elif t=='int':
        return int(s)
    elif t=='float':
        return float(s)



#return true if problems found
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

def writeServerLog(msg):
    if SERVERLOG_MUTEX.acquire():
        f=open('serverLog_TCP.txt','a')
        f.writelines(msg+'\n')
        f.close()
        SERVERLOG_MUTEX.release()

def consoleMSG_handler(msg):
    print (msg)
    writeServerLog(msg)

def haskey(key):
    if key in set(db.getall()):
        return True
    else:
        return False

def DELETE(k,addr,sock):
    #acquire mutex, delete, dump. print, return
    if DB_MUTEX.acquire():
        if haskey(k):
            db.rem(k)
            db.dump()
            consoleMSG=getTimestamp()+'  INFO: Key '+str(k)+'deleted succeddfully. Requested by:'+str(addr)
            consoleMSG_handler(consoleMSG)
            returnMSG="ServerINFO: Delete "+str(k)+'successfully.'
            sock.sendto(returnMSG,addr)
        else:
            consoleMSG=getTimestamp()+'  WARNING: Key '+str(k)+' does not exist. Requested by:'+str(addr)
            consoleMSG_handler(consoleMSG)
            returnMSG="ServerWARNING: Key"+str(k)+'does not exist.'
            sock.sendto(returnMSG,addr)
        DB_MUTEX.release()

def PUT(k,v,addr,sock):
    #acquire mutex, put, dump. print, return
    if DB_MUTEX.acquire():
        if haskey(k):
            db.set(k,v)
            db.dump()
            #default policy is to overwrite            
            consoleMSG=getTimestamp()+'  WARNING: Key '+str(k)+' has been overwritten. Requested by:'+str(addr)
            consoleMSG_handler(consoleMSG)
            returnMSG="ServerWARNING: Key"+str(k)+' has been overwritten.'
            sock.sendto(returnMSG,addr)
        else:
            db.set(k,v)
            db.dump()
            consoleMSG=getTimestamp()+'  INFO: Key '+str(k)+' put successfully. Requested by:'+str(addr)
            consoleMSG_handler(consoleMSG)
            returnMSG="ServerINFO: Key"+str(k)+' put successfully.'
            sock.sendto(returnMSG,addr)          
        DB_MUTEX.release()

def GET(k,addr,sock):
    if DB_MUTEX.acquire():
        #acquire mutex, get, dump. print, return
        if haskey(k):
            v=db.get(k)
            consoleMSG=getTimestamp()+'  INFO: Key '+str(k)+' get successfully. Requested by:'+str(addr)
            consoleMSG_handler(consoleMSG)
            returnMSG=json.dumps({k:v})
            sock.sendto(returnMSG,addr)          
        else:
            consoleMSG=getTimestamp()+'  ERROR: Key '+str(k)+' does not exist. Requested by:'+str(addr)
            consoleMSG_handler(consoleMSG)
            returnMSG="ServerERROR: Key"+str(k)+'does not exist.'
            sock.sendto(returnMSG,addr)
        DB_MUTEX.release()


def worker(data,addr,sock):
        #check data integrity and parse data
    tmp=data.split(',')      
    cmd=str(tmp[0]).strip()

    if checkDataIntegrity(cmd,tmp):
        consoleMSG=getTimestamp()+'  ERROR: Recieved malformated data from: '+str(addr)
        consoleMSG_handler(consoleMSG)
        sock.sendto("ServerERROR:Request is malformatted.",addr)
            
    else:
        tmp[1]=convertType(tmp[1],tmp[2])   
        if cmd=='PUT':
            tmp[3]=convertType(tmp[3],tmp[4])
            PUT(tmp[1],tmp[3],addr,sock)                    
        elif cmd=='GET':
            GET(tmp[1],addr,sock)
        elif cmd=='DELETE':
            DELETE(tmp[1],addr,sock)

                
                

    
    

if __name__ == "__main__":  

    #try to parse args
    try:
        a,b=sys.argv[1].strip().split(':')
        #input validation
        assert(len(a.split('.'))==4)
        SERVER_IP=str(a)
        SERVER_PORT=int(b)
    except Exception,e:
        print "Error: Input Syntax Error. Please check your Input."
        sys.exit(0)

    #try to start
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.bind((SERVER_IP,SERVER_PORT))  
        #s.listen(5)
    except Exception,e:
        print 'Error: Failed to start server.'
        print e
        sys.exit(0)

    consoleMSG=getTimestamp()+"  INFO:Server Started Successfully! Waiting for connection."
    consoleMSG_handler(consoleMSG)

    #load database
    try:
        db=pk.load('KVStore3.db',False)
        consoleMSG=getTimestamp()+"  INFO:Database Loaded Successfully!"
        consoleMSG_handler(consoleMSG)
    except Exception,e:
        consoleMSG=getTimestamp()+"  ERROR: Failed to load Databse!"
        consoleMSG_handler(consoleMSG)
        sys.exit(0)

    while True:
        data,addr=s.recvfrom(1024)
        #if a client has sent a request for connection, then open a new thread to handle
        if data!=None:
            t=threading.Thread(target=worker,args=(data, addr,s))
            t.start()
            data=None


