import socket 
from threading import Thread 
from SocketServer import ThreadingMixIn 
import time
import threading
import json, sys
import logging
import random
import csv
import copy

######################Constants######################
REQVOTE = 'RequestVote'
RESVOTE = 'ResponseVote'
APPENDENTRIES = 'AppendEntries'
RESENTRIES = 'ResponseEntries'
CLIREQ = 'ClientRequest'
SHOWREQ = 'ShowRequest'
CONFIGCHANGE = 'ConfigChangeRequest'

PHASE1 = -1
PHASE2 = -2

STATES = {1: 'FOLLOWER', 2: 'CANDIDATE', 3: 'LEADER'}

######################################################

class RaftServer():
    def __init__(self, dcId):
        self.dcId = dcId
        self.electionTimer = None
        self.heartbeatTimer = None
        self.voteCount = 0
        self.logEntries = []
        self.oldConfig = None
        self.newConfig = None

        # self.state = STATES[1]
        # self.term = 0
        # self.leaderId = None
        # self.votedFor = {}
        # self.followers = {}
        # self.commitIdx = -1
        # self.logEntries = []
        # self.replicatedIndexCount = {}
        self.initParam()
        self.readAndApplyConfig()
        self.resetElectionTimer()
        self.startServer()


    def initParam(self):
        '''Read from log file and update in memory variables based on last log entry'''
        #Optimize
        self.initState()
        self.initLogEntries()


    def initState(self):
        with open(self.dcId +'_state.json') as state_file:    
            state = json.load(state_file)

        self.state = state['state']
        self.term = state['term']
        self.leaderId = state['leaderId']
        self.votedFor = state['votedFor']
        self.followers = state['followers']
        self.commitIdx = state['commitIdx']
        self.replicatedIndexCount = state['replicatedIndexCount']
        self.tickets = state['tickets']
        self.configFile = state['configFile']
        

    def initLogEntries(self):
        '''Initialize log realted variables from the .log file'''
        
        with open(str(dcId)+'.log') as log_file:
            reader = csv.reader(log_file, delimiter=',', quoting=csv.QUOTE_NONE)
    
            for entry in reader:
                '''Convert idx, term and tickets to integers and append to logEntries variable'''
                entry[0], entry[1], entry[2] = \
                int(entry[0]), int(entry[1]), int(entry[2])
                self.logEntries.append(entry)

        '''From lastLog, initialize lastLogIdx and lastLogTerm values'''
        lastLog = self.logEntries[-1] if self.logEntries else None
        if not lastLog:
            lastLog = [-1, 0, None, None]
    
        self.lastLogIdx = lastLog[0]
        self.lastLogTerm = lastLog[1]


    def readAndApplyConfig(self):
        '''Read from config file and update in memory variables'''
        with open(self.configFile) as config_file:    
            self.config = json.load(config_file)

        self.election_timeout = self.config['election_timeout']
        self.heartbeat_timeout = self.config['heartbeat_timeout']
        dcInfo= {'dc_name':self.dcId.upper()}
        self.logger = self.logFormatter(dcInfo)
        totalDcs = len(self.config["datacenters"])
        self.majority = totalDcs/2 + 1


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


    def writeLogEntriesToFile(self):
        with open(self.dcId+".log", "w") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONE)
            writer.writerows(self.logEntries)


    def writeStateToFile(self):
        state = {
            "state":self.state,
            "term":self.term,
            "leaderId":self.leaderId,
            "votedFor": self.votedFor,
            "followers":self.followers,
            "commitIdx":self.commitIdx,
            "replicatedIndexCount":self.replicatedIndexCount,
            "tickets":self.tickets,
            "configFile":self.configFile
        }

        with open(self.dcId +'_state.json', 'w') as fp:
            json.dump(state, fp, sort_keys=True, indent=4)
        

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
            'voteGranted': voteGranted,
            'dcId':self.dcId
            }
        }
        return msg


    def formAppendEntriesMsg(self, nextIdx):
        msg = {
        'AppendEntries' : {
            'term': self.term,
            'leaderId': self.dcId,
            'prevLogIdx': self.logEntries[nextIdx-1][0] if nextIdx>0 else -1,
            'prevLogTerm': self.logEntries[nextIdx-1][1] if nextIdx>0 else 0,
            'entries': self.getLogEntries(nextIdx),
            'commitIdx': self.commitIdx
            }
        }
        return msg


    def formResponseEntriesMsg(self, success=False):
        msg = { 
        'ResponseEntries': {
            'term': self.term,
            'followerId':self.dcId,
            'lastLogIdx':self.lastLogIdx,
            'success': success
            }
        }
        return msg


    def formClientResponseMsg(self, success=False, redirect=False, respMsg=None):
        msg = { 
        'ClientResponse': {
            'leaderId': self.leaderId,
            'success': success,
            'redirect':redirect,
            'response':respMsg
            }
        }
        return msg


    def formShowResponseMsg(self, respMsg=None):
        msg = { 
        'ShowResponse': {
            'leaderId': self.leaderId,
            'response':respMsg
            }
        }
        return msg


    def getLogEntries(self, nextIdx):
        '''Send everything from nextIdx to lastIdx; if they are equal, send empty list for heartbeat'''
        return self.logEntries[nextIdx:]

    ############################# Leader election methods#############################

    def resetElectionTimer(self):
        '''Start a timer; when it goes off start the election'''
        if self.electionTimer:
            self.electionTimer.cancel()

        timeout = random.uniform(self.election_timeout[0], self.election_timeout[1])
        #print 'Timeout is %.2f' %timeout
        self.electionTimer = threading.Timer(timeout, self.startElection)
        self.electionTimer.start()


    def startElection(self):
        '''If not already a leader, change to candidate, increment term and req vote'''
        if not self.state == STATES[3]:
            self.voteCount = 0
            self.state = STATES[2]
            self.term += 1
            self.votedFor[self.term] = self.dcId
            self.voteCount += 1
            self.requestVote()
            self.writeStateToFile()
            

    def requestVote(self):
        reqMsg = self.formRequestVoteMsg()
        self.resetElectionTimer()
        for dcId in self.config["datacenters"]:
            if dcId == self.dcId:
                continue
            ip, port = self.config["datacenters"][dcId][0], self.config["datacenters"][dcId][1]
            self.sendTcpMsg(ip, port, reqMsg)


    def handleVoteRequest(self, msg):
        grantingVote = False
        ip, port = self.getServerIpPort(msg['candidateId'])

        if msg['term'] > self.term:
            '''Update term if it is lesser than candidat'e term'''
            self.term = msg['term']
            self.convertToFollower()

        if msg['term'] >= self.term:
            '''Check if log entries of candidate is good enough to be elected as leader'''
            if (msg['term'] not in self.votedFor) or (self.votedFor[msg['term']] == msg['candidateId']):
                if (msg['lastLogTerm'] > self.lastLogTerm or \
                    (msg['lastLogTerm'] == self.lastLogTerm and msg['lastLogIdx'] >= self.lastLogIdx)):
                    '''Candidate's log is at least as much as voter's log'''
                    respMsg = self.formResponseVoteMsg(voteGranted=True)
                    self.sendTcpMsg(ip, port, respMsg)
                    grantingVote = True
                    self.votedFor[msg['term']] = msg['candidateId']
                    self.convertToFollower()

        self.writeStateToFile()
        if not grantingVote:
            '''If conditions for granting vote failed, respond with "no"'''
            respMsg = self.formResponseVoteMsg(voteGranted=False)
            self.sendTcpMsg(ip, port, respMsg)


    def handleVoteReply(self, msg):
        '''If vote is granted and you get majority of votes, convert to 
        leader else update term and convert to follower.'''
        if msg['voteGranted'] == True:
            self.voteCount += 1
            if self.voteCount >= self.majority and self.state != STATES[3]:
                self.convertToLeader()
        elif msg['term'] > self.term:
            self.term = msg['term']
            self.convertToFollower()


    def convertToLeader(self):
        self.logger.debug('Converting to leader.')
        self.state = STATES[3]
        self.leaderId = self.dcId
        self.initFollowerDetails()
        self.sendAppendEntriesToAll()
        self.resetHeartbeatTimer()
        self.writeStateToFile()


    ############################# Leader responsibilities methods#############################

    def resetHeartbeatTimer(self):
        '''Start a timer; keep sending hearbeats after it goes off'''
        if self.heartbeatTimer:
            self.heartbeatTimer.cancel()
        self.heartbeatTimer = threading.Timer(self.heartbeat_timeout, self.sendAppendEntriesToAll)
        self.heartbeatTimer.start()


    def initFollowerDetails(self):
        '''Initialize next index for every follower once the server becomes leader'''
        for dcId in self.config['datacenters']:
            if dcId == self.dcId:
                continue
            if dcId not in self.followers:
                '''Initialize nextIdx for each follower as leader's lastIdx+1.'''
                self.followers[dcId] = self.lastLogIdx + 1

        '''If there are any servers that are removed in the current config, 
        remove it from follower list'''
        currentFollowers = self.followers.keys()
        for dcId in currentFollowers:
            if dcId not in self.config['datacenters']:
                self.followers.pop(dcId)


    def sendAppendEntriesToAll(self):
        for dcId in self.followers:
            self.sendAppendEntriesMsg(dcId)
        self.resetHeartbeatTimer()


    def sendAppendEntriesMsg(self, dcId, display=True):
        msg = self.formAppendEntriesMsg(self.followers[dcId])
        ip, port = self.getServerIpPort(dcId)
        if len(msg['AppendEntries']['entries']) == 0:
            display = False
        self.sendTcpMsg(ip, port, msg, display=display)


    def handleResponseEntries(self, msg):
        if msg['term'] > self.term:
            '''There is new leader; step down'''
            self.convertToFollower()
        else:
            if msg['success'] == True:
                self.updateReplicationCount(msg)
            else:
                self.retryAppendEntries(msg)

            self.writeStateToFile()


    def retryAppendEntries(self, msg):
        '''Consistency check has failed for this follower.
        Decrement it's nextIdx and retry appendEntries RPC'''
        followerId = msg['followerId']
        self.followers[followerId] -= 1
        self.sendAppendEntriesMsg(followerId)


    def updateReplicationCount(self, msg):
        followerId = msg['followerId']
        nextIdx = self.followers[followerId]
        
        '''Starting from nextIdx of the follower who responded till its lastIdx, updated 
        replicated count of new entries that hasn't gotten majority yet'''
        while nextIdx <= msg['lastLogIdx']:
            if nextIdx in self.replicatedIndexCount:
                self.replicatedIndexCount[nextIdx] += 1
                if self.replicatedIndexCount[nextIdx] >= self.majority:
                    self.updateCommitIdx(nextIdx)
            nextIdx += 1

        self.followers[followerId] = nextIdx   


    def updateCommitIdx(self, nextIdx):
        '''Get the term for the entry that just got majority'''
        logTerm = self.logEntries[nextIdx][1]

        '''If the term of the entry is same as current leader's term, then mark all 
        entries till that entry as committed'''
        if logTerm == self.term:
            while self.commitIdx < nextIdx:
                self.commitIdx += 1
                self.logger.debug('Updated commited index to %d' %self.commitIdx)
                self.replicatedIndexCount.pop(self.commitIdx)
                self.checkAndCommitConfigChange(self.commitIdx)
                '''Once an entry is commited, update ticket count and respond to client'''
                self.executeClientRequest(self.commitIdx, respondToClient=True)
                


    def checkAndCommitConfigChange(self, commitIdx):
        cmd, reqId = self.getClientRequestFromLog(self.commitIdx)
        if cmd == PHASE1:
            self.handleConfigChange(PHASE2, reqId)
        elif cmd == PHASE2:
            response = 'Successfully changed configuration.'
            respMsg = self.formClientResponseMsg(success=True, redirect=False, respMsg=response)
            self.replyToClient(reqId, respMsg)

    ############################# Follower functionalities methods #############################

    def handleAppendEntries(self, msg):
        '''Reset election timer as leader is alive'''
        success = False
        if self.term > msg['term']:
            '''Invalid leader; just return failure RPC to update stale leader'''
            success = False

        else:
            self.term = msg['term']
            self.leaderId = msg['leaderId']
            self.convertToFollower()

            '''Perform consistency check on logs of follower and leader'''
            #Verify***
            if self.lastLogIdx < msg['prevLogIdx']:
                '''Missing entries case: send failure so that leader decrements next index and retries'''
                success = False

            else:
                if msg['prevLogIdx'] >= 0 and self.logEntries[msg['prevLogIdx']][1] != msg['prevLogTerm']:
                    '''Inconsistent entries case: At prevLogIdx, follower's and leader's terms don't match.
                    Send failure so that leader decrements next idx and retries'''
                    success = False
                else:
                    '''Success case: Keep entries only till prevLogIdx, to that append the newly sent entries'''
                    self.logEntries = self.logEntries[:msg['prevLogIdx']+1]
                    self.logEntries.extend(msg['entries'])
                    if len(msg['entries']) > 0:
                        self.lastLogIdx = len(self.logEntries)-1
                        self.lastLogTerm = self.logEntries[self.lastLogIdx][1]
                        self.checkForConfigChange(msg['entries'])
                    success = True
                    if msg['commitIdx'] > self.commitIdx:
                        self.updateCommitIdxOfFollower(msg['commitIdx'])

                    if len(msg['entries']) > 0:
                        self.logger.debug('Updated (lastLogIdx, lastLogTerm, logEntries) to (%d, %d, %s)' \
                        %(self.lastLogIdx, self.lastLogTerm, repr(self.logEntries)))

        if len(msg['entries']) > 0 or success==False:
            '''Respond only for msgs that are not heartbeats'''
            respMsg = self.formResponseEntriesMsg(success=success)
            ip, port = self.getServerIpPort(msg['leaderId'])
            self.sendTcpMsg(ip, port, respMsg)

        self.writeStateToFile()
        self.writeLogEntriesToFile()


    def updateCommitIdxOfFollower(self, newCommitIdx):
        '''Update commit idx and state machine of follower'''
        while self.commitIdx < newCommitIdx:
            self.commitIdx += 1
            self.logger.debug('Updated commited index to %d' %self.commitIdx)
            '''Once an entry is commited, update ticket count and respond to client'''
            self.executeClientRequest(self.commitIdx)


    def convertToFollower(self):
        self.state = STATES[1]
        self.voteCount = 0
        self.followers = {}
        self.replicatedIndexCount = {}
        self.resetElectionTimer()
        self.writeStateToFile()

    ############################# Client request methods #############################

    def validRequest(self, requestedTickets):
        '''Chech if there enough tickets to serve the clients request'''
        if requestedTickets <= self.tickets:
            return True
        return False


    def handleClientRequest(self, recvMsg, msg):
        if not self.state == STATES[3]:
            '''This server is not leader; reply client with redirect message'''
            response = 'Current leader is %s. Please redirect request to server %s' %(self.leaderId, self.leaderId)
            respMsg = self.formClientResponseMsg(success=False, redirect=True, respMsg=response)
            self.replyToClient(msg['reqId'], respMsg)

        else:
            if self.validRequest(msg['tickets']):
                #TODO: Check in logs for existing req
                self.lastLogIdx += 1
                self.lastLogTerm = self.term
                entry = self.getNextLogEntry(msg['tickets'], msg['reqId'])
                self.logEntries.append(entry)
                '''Initialize count for this index as 1 in replicatedIndexCount variable'''
                self.replicatedIndexCount[self.lastLogIdx] = 1
                self.sendAppendEntriesToAll()
                self.writeLogEntriesToFile()
            else:
                '''Client requested too many tickets; repond with appropriate message'''
                response = 'Total tickets available: '+str(self.tickets)+'.'
                response += ' Tickets requested should be less that total tickets available.'
                respMsg = self.formClientResponseMsg(success=False, redirect=False, respMsg=response)
                self.replyToClient(msg['reqId'], respMsg)


    def getNextLogEntry(self, command, reqId):
        #TODO: Storing result on request?
        return [self.lastLogIdx, self.term, command, reqId]


    def executeClientRequest(self, idx, respondToClient=False):
        '''Actual fucntion that decrements no. of tickets in the pool.
        This fucntion is called only when majority of the followers have responded.'''

        requestedTickets, reqId = self.getClientRequestFromLog(idx)
        if requestedTickets > 0:
            self.tickets -= requestedTickets

            if respondToClient:
                response = 'Successfully purchased %s tickets.' %requestedTickets    
                respMsg = self.formClientResponseMsg(success=True, redirect=False, respMsg=response)
                self.replyToClient(reqId, respMsg)


    def handleShowCommand(self, msg):
        response = 'Current log that is present on server %s is:\n' %(self.dcId)
        
        for entry in self.logEntries:
            cmd = entry[2]
            if cmd == -1:
                logResp = str(entry[0]) + ': Config change (old + new).\n'
            elif cmd == -2:
                logResp = str(entry[0]) + ': Config change (new).\n'
            else:
                clientId = entry[3].split(':')[0]
                logResp = str(entry[0]) + ': Client %s bought %d tickets successfully.\n' %(clientId, cmd)

            response += logResp
        respMsg = self.formShowResponseMsg(respMsg=response)
        self.replyToClient(msg['reqId'], respMsg)


    ############################# Config change methods #############################

    def handleConfigChange(self, phase, reqId):
        '''Add entry to log, update config and send AppendEntriesRPC to all'''
        self.lastLogIdx += 1
        self.lastLogTerm = self.term
        entry = self.getNextLogEntry(phase, reqId)
        self.logEntries.append(entry)
        '''Initialize count for this index as 1 in replicatedIndexCount variable'''
        self.replicatedIndexCount[self.lastLogIdx] = 1
        self.updateConfig(phase)
        self.sendAppendEntriesToAll()
        self.writeLogEntriesToFile()


    def updateConfig(self, phase):
        '''For PHASE1 read the changed config from file and store in a variable.
        For PHASE2 just move to the stored values'''
        if phase == PHASE1:
            self.oldConfig = copy.deepcopy(self.config)
            self.readAndApplyNewConfig()
        else:
            self.moveToNewConfig()

        self.writeStateToFile()


    def readAndApplyNewConfig(self):
        '''Read from config file and update in memory variables'''
        with open(self.configFile) as config_file:    
            self.newConfig = json.load(config_file)

        '''From the newly read config, update my current config such that it
        is old + new config'''
        for dcId in self.newConfig['datacenters']:
             if dcId not in self.config:
                self.config['datacenters'][dcId] = self.newConfig['datacenters'][dcId]

        if self.state == STATES[3]:
            '''If the server is current leader, it should add newly added followers to the 
            follower list and set their nextIdx'''
            self.initFollowerDetails()

        self.config['clients'] = self.newConfig['clients']
        oldDcs = len(self.oldConfig["datacenters"])
        newDcs = len(self.newConfig["datacenters"])
        self.majority = (oldDcs+newDcs)/2 


    def moveToNewConfig(self):
        '''Since newConfig contains the right config, move current config to new one
        and update majority'''
        if self.newConfig:
            self.config['datacenters'] = self.newConfig['datacenters']
        totalDcs = len(self.config["datacenters"])
        self.majority = (totalDcs)/2 + 1
        self.oldConfig, self.newConfig = None, None
        '''If there are any servers that are deleted, update followers accordingly'''
        self.initFollowerDetails()


    def checkForConfigChange(self, newEntries):
        '''Go through all newly sent entries, check if any of them are config changes
        for phase1 or phase2, and update config accordingly'''
        for entry in newEntries:
            cmd = entry[2]
            if cmd == PHASE1:
                self.updateConfig(PHASE1)
            elif cmd == PHASE2:
                self.updateConfig(PHASE2)

    ############################# Misc methods #############################

    def sendTcpMsg(self, ip, port, msg, display=True):
        try:
            tcpClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcpClient.settimeout(1)
            tcpClient.connect((ip, port))
            tcpClient.send(json.dumps(msg))
            if display:
                logMsg = 'Sent message to: (%s, %d). Message is: %s' %(ip, port, msg)
                self.logger.debug(logMsg)

        except Exception as e:
            '''When a site is down, tcp connect fails and raises exception; catching and 
            ignoring it as we don't care about sites that are down'''
            pass
        

    def replyToClient(self, reqId, respMsg):
        clientId = reqId.split(':')[0]
        ip, port = self.getClienrIpPort(clientId)
        self.sendTcpMsg(ip, port, respMsg)


    def getServerIpPort(self, dcId):
        '''Get ip and port on which server is listening from config'''
        return self.config['datacenters'][dcId][0], self.config['datacenters'][dcId][1]


    def getClienrIpPort(self, clId):
        '''Get ip and port on which client is listening from config'''
        return self.config['clients'][clId][0], self.config['clients'][clId][1]


    def extractTermFromLog(self, logIdx):
        '''Implement from log'''
        return self.lastLogTerm


    def getClientRequestFromLog(self, logIdx):
        return self.logEntries[logIdx][2], self.logEntries[logIdx][3] 


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
            

            msgType, msg = self.parseRecvMsg(recvMsg)
            if msgType != APPENDENTRIES and msgType != RESENTRIES:
                logMsg = 'Received message from: (%s:%d). Message is: %s' %(self.ip, self.port, recvMsg)
                self.raft.logger.debug(logMsg)

            if msgType == CLIREQ:
                cliReq = True
                self.raft.handleClientRequest(recvMsg, msg)
            elif msgType == SHOWREQ:
                cliReq = True
                self.raft.handleShowCommand(msg)
            elif msgType == REQVOTE:
                self.raft.handleVoteRequest(msg)
            elif msgType == RESVOTE:
                self.raft.handleVoteReply(msg)
            elif msgType == APPENDENTRIES:
                self.raft.handleAppendEntries(msg)
            elif msgType == RESENTRIES:
                self.raft.handleResponseEntries(msg)
            elif msgType == CONFIGCHANGE:
                self.configFile = msg['configFile'] 
                self.raft.handleConfigChange(PHASE1, msg['reqId'])

            if not cliReq:
                conn.close() 
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
            newthread = self.ConnectionThread(conn, cliIP, cliPort, self) 
            newthread.start()
    
 
dcId = sys.argv[1]
delay = int(sys.argv[2])
time.sleep(delay)
raftSrvr = RaftServer(dcId)