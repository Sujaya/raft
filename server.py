import socket 
from threading import Thread 
from SocketServer import ThreadingMixIn 
import time
import threading
import json, sys
import logging
import random


######################Constants######################
REQVOTE = 'RequestVote'
RESVOTE = 'ResponseVote'
APPENDENTRIES = 'AppendEntries'
CLIREQ = 'CLIREQ'

STATES = {1: 'FOLLOWER', 2: 'CANDIDATE', 3: 'LEADER'}

######################################################

class RaftServer():
    def __init__(self, dcId):
        self.state = STATES[1]
        self.term = 0
        self.dcId = dcId
        self.electionTimer = None
        self.heartbeatTimer = None
        self.voteCount = 0
        self.votedFor = None
        self.followers = {}
        self.commitIdx = -1
        self.logEntries = ['1','2']
        self.readAndApplyConfig()
        self.initParam()
        self.resetElectionTimer()
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
        self.majority = self.totalDcs/2 + 1


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


    ############################# Message forming methods#############################

    def formRequestVoteMsg(self):
        msg = { 
        'RequestVote': {
            'candidateId': self.dcId,
            'term': self.term,
            'lastLogIdx': self.lastLogIdx,
            'lastLogTerm': self.lastLogTerm
            }
        }
        return msg


    def formResponseVoteMsg(self, voteGranted=False):
        msg = { 
        'ResponseVote': {
            'term': self.term,
            'voteGranted': voteGranted
            }
        }
        return msg


    def formAppendEntriesMsg(self, nextIdx):
        msg = {
            'AppendEntries' : {
            'term': self.term,
            'leaderId': self.dcId,
            'prevLogIdx': self.lastLogIdx,
            'prevLogTerm': self.lastLogIdx,
            'entries': self.getLogEntries(nextIdx),
            'commitIdx': self.commitIdx
            }
        }
        return msg


    ############################# Leader election methods#############################

    def resetElectionTimer(self):
        '''Start a timer; when it goes off start the election'''
        if self.electionTimer:
            print 'Canceling timer thread id %s' %(self.electionTimer)
            self.electionTimer.cancel()
        timeout = random.uniform(self.election_timeout[0], self.election_timeout[1])
        self.logger.debug('Resetting timer to value %.2f' %timeout)

        if self.dcId == 'dc1': delay = 5
        else: delay = 15
        self.electionTimer = threading.Timer(delay, self.startElection)
        print 'Created timer thread id %s' %(self.electionTimer)
        self.electionTimer.start()


    def startElection(self):
        if not self.state == STATES[2]:
            self.state = STATES[2]
            self.term += 1
            self.requestVote()


    def requestVote(self):
        reqMsg = self.formRequestVoteMsg()
        self.voteCount += 1
        self.resetElectionTimer()
        for dcId in self.config["datacenters"]:
            if dcId == self.dcId:
                continue
            ip, port = self.config["datacenters"][dcId][0], self.config["datacenters"][dcId][1]
            self.sendTcpMsg(ip, port, reqMsg)


    def handleVoteRequest(self, msg):
        grantingVote = False
        ip, port = self.getServerIpPort(msg['candidateId'])

        if msg['term'] >= self.term:
            '''Update term if it is lesser than candidat'e term'''
            self.term = msg['term']
            self.convertToFollower()

            if self.votedFor == None or self.votedFor == msg['candidateId']:
                if (msg['lastLogTerm'] > self.lastLogTerm or \
                    (msg['lastLogTerm'] == self.lastLogTerm and msg['lastLogIdx'] >= self.lastLogIdx)):
                    '''Candidate's log is at least as much as voter's log'''
                    respMsg = self.formResponseVoteMsg(voteGranted=True)
                    self.sendTcpMsg(ip, port, respMsg)
                    grantingVote = True
                    self.resetElectionTimer()

        if not grantingVote:
            '''If conditions for granting vote failed, respond with "no"'''
            respMsg = self.formResponseVoteMsg(voteGranted=False)
            self.sendTcpMsg(ip, port, respMsg)


    def handleVoteReply(self, msg):
        '''If vote is granted and you get majority of votes, convert to 
        leader else update term and convert to follower.'''
        if msg['voteGranted'] == True:
            self.voteCount += 1
            if self.voteCount == self.majority:
                self.convertToLeader()
        elif msg['term'] > self.term:
            self.term = msg['term']
            self.convertToFollower()


    def convertToLeader(self):
        self.logger.debug('Converting to leader.')
        self.initFollowerDetails()
        self.resetHeartbeatTimer()


    ############################# Leader responsibilities methods#############################

    def resetHeartbeatTimer(self):
        '''Start a timer; keep sending hearbeats after it goes off'''
        self.heartbeatTimer = threading.Timer(self.heartbeat_timeout, self.sendAppendEntries)
        self.heartbeatTimer.start()


    def getLogEntries(self, nextIdx):
        '''Send everything from nextIdx to lastIdx; if they are equal, send empty list for heartbeat'''
        return self.logEntries[nextIdx:]


    def initFollowerDetails(self):
        '''Initialize next index for every follower once the server becomes leader'''
        for dcId in self.config['datacenters']:
            if dcId == self.dcId:
                continue
            self.followers[dcId] = self.lastLogIdx + 1 


    def sendAppendEntries(self):
        for dcId in self.followers:
            msg = self.formAppendEntriesMsg(self.followers[dcId])
            ip, port = self.getServerIpPort(dcId)
            self.sendTcpMsg(ip, port, msg)


    ############################# Follower functionalities methods#############################

    def handleAppendEntries(self, msg):
        '''Reset election timer as leader is alive'''
        self.resetElectionTimer()


    ############################# Misc methods#############################

    def sendTcpMsg(self, ip, port, msg):
        tcpClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        tcpClient.connect((ip, port))
        tcpClient.send(json.dumps(msg))
        logMsg = 'Sent message to: (%s, %d). Message is: %s' %(ip, port, msg)
        self.logger.debug(logMsg)


    def convertToFollower(self):
        self.logger.debug('Converting to follower.')
        self.state = STATES[1]
        self.voteCount = 0


    def getServerIpPort(self, dcId):
        '''Get ip and port on which server is listening from config'''
        return self.config['datacenters'][dcId][0], self.config['datacenters'][dcId][1]



    # Multithreaded Python server : TCP Server Socket Thread Pool
    class ConnectionThread(Thread): 
     
        def __init__(self, conn, ip, port, raft): 
            Thread.__init__(self) 
            self.ip = ip
            self.port = port
            self.conn = conn
            self.raft = raft


        def run(self): 
            cliReq = False
            conn, recvMsg = self.conn, self.conn.recv(2048)
            logMsg = 'Received message from: (%s:%d). Message is: %s' %(self.ip, self.port, recvMsg)
            self.raft.logger.debug(logMsg)

            msgType, msg = self.parseRecvMsg(recvMsg)

            if msgType==CLIREQ:
                cliReq = True
                self.raft.handleClientReq()
            elif msgType == REQVOTE:
                self.raft.handleVoteRequest(msg)
            elif msgType == RESVOTE:
                self.raft.handleVoteReply(msg)
            elif msgType == APPENDENTRIES:
                self.raft.handleAppendEntries(msg)

            # if not cliReq:
            #     conn.close() 
            sys.exit()


        def parseRecvMsg(self, recvMsg):
            recvMsg = json.loads(recvMsg)
            msgType, msg = recvMsg.keys()[0], recvMsg.values()[0]
            return msgType, msg


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
            newthread = self.ConnectionThread(conn, cliIP, cliPort, self) 
            newthread.start()
    
 
dcId = sys.argv[1]
delay = int(sys.argv[2])
time.sleep(delay)
raftSrvr = RaftServer(dcId)