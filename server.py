import socket 
from threading import Thread 
from SocketServer import ThreadingMixIn 
import time
import threading
import json, sys
import logging
import random


######################Constants######################
REQ = 'REQ'
REL = 'REL'
REP = 'REP'
CLIREQ = 'CLIREQ'

STATES = {1: 'FOLLOWER', 2: 'CANDIDATE', 3: 'LEADER'}

######################################################

class RaftServer():
    def __init__(self, dcId):
        self.state = STATES[1]
        self.term = 0
        self.dcId = dcId
        self.timer = None
        self.voteCount = 0
        self.readAndApplyConfig()
        self.initParam()
        self.resetTimer()
        self.startServer()


    def initParam(self):
        '''Read from log file and update in memory variables based on last log entry'''
        #Optimize
        with open(str(dcId)+'.log') as log_file:    
            log = list(log_file)[-1] if list(log_file) else None

        if not log:
            parts = [0, 0, None]
        else: 
            parts = log.split(' ')
        self.lastLogTerm = parts[0]
        self.lastLogIdx = parts[1]
        self.lastCommand = parts[2]


    def readAndApplyConfig(self):
        '''Read from config file and update in memory variables'''
        with open('server_config.json') as config_file:    
            self.config = json.load(config_file)

        self.election_timeout = self.config['election_timeout']
        self.heartbeat_timeout = self.config['heartbeat_timeout']
        dcInfo= {'dc_name':self.dcId.upper()}
        self.logger = self.logFormatter(dcInfo)
        self.totalDcs = len(self.config["datacenters"])


    def logFormatter(self, dcInfo):
        '''Logging info'''
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(dc_name)s: %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger = logging.LoggerAdapter(logger, dcInfo)
        return logger


    def resetTimer(self):
        '''Start a timer; when it goes off start the election'''
        if self.timer:
            self.timer.cancel()
        timeout = random.uniform(self.election_timeout[0], self.election_timeout[1])
        self.timer = threading.Timer(timeout, self.startElection)
        self.timer.start()


    def startElection(self):
        self.state = STATES[2]
        self.term += 1
        time.sleep(5)
        self.requestVote()


    def formRequestVoteMsg(self):
        msg = {
            'candidateId': self.dcId,
            'term': self.term,
            'lastLogIdx': self.lastLogIdx,
            'lastLogTerm': self.lastLogTerm
        }
        return msg


    def requestVote(self):
        reqMsg = self.formRequestVoteMsg()
        self.voteCount += 1
        self.resetTimer()
        for dcId in self.config["datacenters"]:
            if dcId == self.dcId:
                continue
            ip, port = self.config["datacenters"][dcId][0], self.config["datacenters"][dcId][1]
            self.sendTcpMsg(ip, port, reqMsg)


    def sendTcpMsg(self, ip, port, msg):
        tcpClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        tcpClient.connect((ip, port))
        tcpClient.send(json.dumps(msg))
        logMsg = 'Sent message to: (%s, %d). Message is: %s' %(ip, port, msg)
        self.logger.debug(logMsg)


    # Multithreaded Python server : TCP Server Socket Thread Pool
    class ConnectionThread(Thread): 
     
        def __init__(self, conn, ip, port): 
            Thread.__init__(self) 
            self.ip = ip 
            self.port = port 
            self.conn = conn


    def startServer(self):
        ip, port = self.config["datacenters"][self.dcId][0], self.config["datacenters"][self.dcId][1]

        tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        tcpServer.bind((ip, port))

        print 'Server ready to listen on (%s:%d)' %(ip, port)
        while True: 
            tcpServer.listen(4) 
            (conn, (cliIP,cliPort)) = tcpServer.accept()
            print 'Accepted coonection from %s' %repr(conn)
            newthread = self.ConnectionThread(conn, cliIP, cliPort) 
            newthread.start()
    
 
dcId = sys.argv[1]
raftSrvr = RaftServer(dcId)