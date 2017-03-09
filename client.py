import socket
import time
import logging
import json, sys
import random
import threading
from threading import Thread
logging.basicConfig(filename='client.log',level=logging.DEBUG)


clientId = sys.argv[1]
BUFFER_SIZE = 2000 
 
class RaftClient():

    def __init__(self, clientId):
        self.leaderId = None
        self.reqId = 0
        self.clientId = clientId
        self.tickets = 0
        self.readAndApplyConfig()
        thread = Thread(target = self.requestTicketsFromUser)
        thread.start()
        #self.requestTicketsFromUser()
        self.startListening()


    def readAndApplyConfig(self):
        with open('config.json') as config_file:    
            self.config = json.load(config_file)
        self.timeout = self.config['client_request_timeout']



    def getServerIpPort(self, dcId):
        '''Get ip and port on which server is listening from config'''
        return self.config['datacenters'][dcId][0], self.config['datacenters'][dcId][1]


    def formRequestMsg(self, tickets):
        msg = { 
        'ClientRequest': {
            'reqId': self.clientId + ':' + str(self.reqId),
            'tickets': tickets
            }
        }
        return msg


    def parseRecvMsg(self, recvMsg):
            ''' msg = {'leaderId': <id>, 'response':<>}'''
            recvMsg = json.loads(recvMsg)
            msgType, msg = recvMsg.keys()[0], recvMsg.values()[0]
            return msgType, msg


    def startListening(self):
        '''Start listening for server response'''
        ip, port = self.config["clients"][self.clientId][0], self.config["clients"][self.clientId][1]

        tcpClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        tcpClient.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        tcpClient.bind((ip, port))

        while True:
            tcpClient.listen(4) 
            (conn, (cliIP,cliPort)) = tcpClient.accept()
            
            data = conn.recv(BUFFER_SIZE)
            self.cancelTimer()
            # while not data:
            #     data = tcpClient.recv(BUFFER_SIZE)
            _, msg = self.parseRecvMsg(data)

            '''Update leader id based on the server response'''
            self.leaderId = msg['leaderId']
            print msg['response']

            if msg['redirect'] == True:
                self.requestTicket()
            else:
                self.requestTicketsFromUser()


    def sendRequest(self):
        '''Form the request message and send a tcp request to server asking for tickets'''
        if not self.leaderId:
            '''If leader is not known, randomly choose a server and request tickets'''
            dcId =  random.randint(1, len(self.config['datacenters']))
            dcId = 'dc'+str(dcId)
        else:
            dcId = self.leaderId

        ip, port = self.getServerIpPort(dcId)
        reqMsg = self.formRequestMsg(self.tickets)
        reqMsg = json.dumps(reqMsg)
        try:
            tcpClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            '''Start timer to get a reply within certain time; if timeout happens, resend the same request'''
            self.startTimer()
            tcpClient.settimeout(1)
            tcpClient.connect((ip, port))
            tcpClient.send(reqMsg)
            time.sleep(0.5)
            tcpClient.close()
        except Exception as e:
            '''When a site is down, tcp connect fails and raises exception; catching and 
            ignoring it as we don't care about sites that are down'''
            pass


    def startTimer(self):
        self.timer = threading.Timer(self.timeout, self.handleTimeout)
        self.timer.start()

    def cancelTimer(self):
        self.timer.cancel()


    def handleTimeout(self):
        '''On timeout, choose a server that is not previous leader and send ticket request'''
        oldLeader = self.leaderId
        while True:
            dcId =  random.randint(1, len(self.config['datacenters']))
            dcId = 'dc'+str(dcId)
            if dcId != oldLeader:
                self.leaderId = dcId
                break
        self.sendRequest()


    def requestTicket(self):
        self.sendRequest()


    def requestTicketsFromUser(self): 
        '''Take request from user and request tickets from server''' 
        while True:
            noOfTickets = raw_input("Enter no. of tickets: ")
            if noOfTickets <= 0:
                print 'Invalid entry! Please enter a valid ticket count.'
                continue
            break

        self.tickets = int(noOfTickets)
        '''Increment request id on each valid user request'''
        self.reqId += 1
        self.sendRequest()
        
        
client = RaftClient(clientId)

