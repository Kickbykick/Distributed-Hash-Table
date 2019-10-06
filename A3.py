###############################################
#COMP 3010, A3
#KIIBATI ADEOJO, 7761684
#DISTRIBUTED HASH TABLE
###############################################
import socket
import json
import select
import sys
import os
import random
import copy
import pprint
import datetime

ROOT_PORT = 14000
ROOT_HOSTNAME = "silicon.cs.umanitoba.ca"
ROOT_ID = 65535
logFile = ""
mainNode = ""
JOIN_STATE = False
MY_PORT = 15014
MY_ID = random.randint( 1, ROOT_ID - 2 )
MY_HOSTNAME = socket.getfqdn()
global mainSocket		


###############################################
#CREATE NODE CLASS
###############################################
class node:
	#Class that defines the current node
	def __init__(self,phname,phport,pID,nname,nport,shname,shport,identity):
		self.predHostName = phname
		self.predPort = phport
		self.predID = pID
		self.nodeHostName = nname
		self.nodePort = nport
		self.succHostName = shname
		self.succPort = shport
		self.nodeID = identity


	#LISTEN FOR ANY REQUEST AND ANY INPUT
	def listensocket(self):
		theString = ""
		socketFD = mainSocket.fileno()

		(readFDs, writeFDs, errorFDs) = select.select( [socketFD, sys.stdin], [], [], 2)
		for descriptor in readFDs:
			if descriptor == sys.stdin:
				input = sys.stdin.readline()
				print input
				findDict = ""

				if input == "\n":
					print "Empty Request, Exiting Program........"
					mainSocket.close()
					sys.exit()
				else:
					try:
						input = int(input)
						if input > MY_ID:
							findDict = {"cmd": "find",
							"port": MY_PORT,
							"ID": MY_ID,
							"hostname": socket.getfqdn(),
							"hops": 0,
							"query":input
						}
						else:
							if input < MY_ID:
								input = random.randint( MY_ID + 1, ROOT_ID )
								print "Input too small.... Generating a big enough random number\n"
								findDict = {"cmd": "find",
								"port": MY_PORT,
								"ID": MY_ID,
								"hostname": socket.getfqdn(),
								"hops": 0,
								"query":input
							}
						findRequest( mainNode.succHostName, mainNode.succPort, findDict)
						
						#Printing Information	
						currentDT = datetime.datetime.now()
						print "Find Query sent to my successor"
						print "TimeStamp: ", (str(currentDT)), "\n"
						print findDict
					except:
						print "Please enter an integer............"	
			elif descriptor == socketFD:
				print "Recieved a message\n"
				theString += os.read(socketFD,1024)
		
		if not theString:
			return theString
		elif not isinstance(theString, dict):
			theString = json.loads(theString)

		return theString


	#PRINT NODES CURRENT INFORMATION
	def printInfo(self):
		#Function to print current node positon
		print "NODE INFORMATION UPDATE\n_____________________________"
		print "Node ID:",self.nodeID
		print "Predecessor:",self.predHostName,":",self.predPort,":",self.predID
		print "This node:",self.nodeHostName,":",self.nodePort
		print "Successor:",self.succHostName,":",self.succPort,"\n"


# UPDATE NODES PREDECESSOR
def predUpdate(jsonRecvdDict,nodeval,conn):
	# Handling predecessor update requests
	nodeval.predHostName = jsonRecvdDict["hostname"]
	nodeval.predPort = int( jsonRecvdDict["port"] )
	nodeval.predID = int( jsonRecvdDict["ID"] )


# UPDATE NODES SUCCESSOR
def succUpdate(jsonRecvdDict,nodeval,conn):
	# Handling successor update requests
	nodeval.succHostName = jsonRecvdDict["hostname"]
	nodeval.succPort = int( jsonRecvdDict["port"] )


# SEARCH FOR WHERE THIS KEY FITS AND RETURN THAT HOSTNAME AND PORT
def searchForSpot(clientHostname, clientPort, senddata, idKey):
	# Function to send requests and responses to specified targets
	jsonFile = json.dumps(senddata) # make JSON FILE
	
	#Prepare Socket for sending INFO
	sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock2.connect((clientHostname , clientPort))
	sock2.sendall(jsonFile)

	#NEEDED VARIABLE
	hostnameTmp = ""
	portTmp = ""
	idTmp = 65535
	stop = False

	while (idTmp > idKey and not stop):
		jsonFile = json.dumps(senddata)
		sock2.connect((clientHostname , clientPort))
		sock2.sendall(jsonFile)	

		if(senddata["cmd"] == "pred?"):
			socketFD = sock2.fileno()
			# print "Sending Pred Request............. \n"
			rcvdDict = pred(socketFD) # returns a DICT

			if(not rcvdDict):
				print "ERROR: A node does not handle \"pred\" request properly"
				stop = True
			else:
				# print rcvdDict
				#Validate My pred Message
				try:
					if( ( (rcvdDict.has_key("thePred") ) ) ):
						if( (rcvdDict["thePred"].has_key("hostname") ) and ( (rcvdDict["thePred"].has_key("ID") ) ) and ( (rcvdDict["thePred"].has_key("port") ) ) ):
							if( ( "cs.umanitoba.ca" in rcvdDict["thePred"]["hostname"] ) and ( int (rcvdDict["thePred"]["ID"] ) >= 0) and ( int (rcvdDict["thePred"]["ID"] ) <= ROOT_ID)  ):
								senddata = {"cmd": "pred?",
										"port": portTmp,
										"ID": idTmp,
										"hostname": hostnameTmp
										}
								hostnameTmp = rcvdDict["thePred"]["hostname"]
								portTmp = int(rcvdDict["thePred"]["port"])
								idTmp = int(rcvdDict["thePred"]["ID"])
							else:
								print "Invalid HostName or Port. Exiting....\n"
								stop = True
				except:
					stop = True

	sock2.close()

	return hostnameTmp, portTmp


# RETURN THE PREDECESSOR
def lookForPred(clientHostname, clientPort, senddata):
	# Function to send requests and responses to specified targets
	jsonFile = json.dumps(senddata) # make JSON FILE
	
	#Prepare Socket for sending INFO
	sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock2.connect((clientHostname , clientPort))
	sock2.sendall(jsonFile)
	socketFD = sock2.fileno()

	rcvdDict = pred(socketFD) # returns a DICT

	sock2.close()

	return rcvdDict


# START A FIND QUERY AND STABILIZATION PROCESS
def findRequest( recvHost, recvPort, jsonDict):
	sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	hostnameTmp = ""
	portTmp = ""
	idTmp = ""
	print "Running FindRequest................................\n"

	#Running Stabilization, Always stabilize before doing find
	senddata = {"cmd": "pred?",
			"port": MY_PORT,
			"ID": MY_ID,
			"hostname":socket.getfqdn()
	}
	rcvdDict = lookForPred( recvHost, recvPort, senddata)
	print rcvdDict

	if( not validateDictionary(rcvdDict) ):
		print "Successor Sent Invalid Message...............Ignoring................\n"
	else:
		hostnameTmp = rcvdDict["thePred"]["hostname"]
		portTmp = int(rcvdDict["thePred"]["port"])
		idTmp = int(rcvdDict["thePred"]["ID"])

	#If I am the correct pred
	if(portTmp == MY_PORT and hostnameTmp == MY_HOSTNAME):
		jsonFile = json.dumps(jsonDict)

		theAddress = (recvHost , recvPort)
		sock2.sendto(jsonFile, theAddress)

		socketFD = sock2.fileno()

		#Sned the Find File Descriptor
		find(socketFD)

		currentDT = datetime.datetime.now()
		print "Find Query sent to my successor"
		print "TimeStamp: ", (str(currentDT)), "\n"
		print jsonDict
	else:
		print "My Successors Pred Is not Me\nRejoining the Ring.............\n"
		joinhandle = nodejoin(ROOT_HOSTNAME, ROOT_PORT, MY_HOSTNAME, MY_PORT, mainNode)

	#Close Socket
	sock2.close()
	print "Done with FindRequest\n"


# JOIN THE RING FROM THE BOOTSTRAP
def nodejoin(rh,rp,oh,op, mainNode):
	JOIN_STATE = True
	# Sending JOIN request to root node
	print "Requesting to Join the ring...........\n"
	jsonDict = {"cmd": "pred?",
				"port": MY_PORT,
				"ID": MY_ID,
				"hostname":socket.getfqdn()
	}

	clientHostname, clientPort = searchForSpot( rh, rp, jsonDict, MY_ID)
	print "This is the Last Person Found", clientPort, " ", clientHostname, " \n"

	jsonDict = {"cmd": "setPred",
				"port": MY_PORT,
				"ID": MY_ID,
				"hostname": socket.getfqdn(),
	}
	
	jsonFile = json.dumps(jsonDict)

	#Join at the last Person
	lastPredSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

	if (clientPort != MY_PORT and clientHostname):
		# print "\nDebug: Setting this clients Pred to be me ", clientPort, " ", clientHostname, " \n"
		lastPredSock.connect((clientHostname , clientPort)) #Connect to newly found pred
		lastPredSock.sendall(jsonFile) #send it over
			
		jsonSuccSet = {"port": clientPort,
						"hostname": clientHostname
		}

		succUpdate(jsonSuccSet,mainNode,lastPredSock)

		print "I just joined the ring:\n"
		print "TimeStamp: ", (str(datetime.datetime.now())), "\n"
		print mainNode.printInfo()
	else:
		print "\nI am the last person found, No pred Set ", " \n"
			
	lastPredSock.close()
	print "\nEnding Join..................I am on the Ring"
	JOIN_STATE = False


# HELPER
def pred(socketFD):
	theString = ""
	socketFD = mainSocket.fileno()

	(readFDs, writeFDs, errorFDs) = select.select( [socketFD, sys.stdin], [], [], 2)
	for descriptor in readFDs:
		if descriptor == sys.stdin:
				print "stdIN"
		elif descriptor == socketFD:
				# print "Pred Sent\n"
				theString += os.read(socketFD,1024)
	
	if not theString:
		return theString
	elif not isinstance(theString, dict):
		theString = json.loads(theString)

	return theString


# HELPER
def find(socketFD):
	theString = ""
	
	socketFD = mainSocket.fileno()
	(readFDs, writeFDs, errorFDs) = select.select( [socketFD, sys.stdin], [], [], 2)
	for descriptor in readFDs:
		if descriptor == sys.stdin:
				print "stdIN"
		elif descriptor == socketFD:
				theString += os.read(socketFD,1024)


# VALIDATE A DICTIONARY INPUT
def validateDictionary(theDict):
	valid = False

	try:
		if( isinstance(theDict, dict) ):
			if( (theDict.has_key("hostname") ) and ( (theDict.has_key("ID") ) ) and ( (theDict.has_key("port") ) ) and ( (theDict.has_key("cmd") ) ) ):
				if( ( "cs.umanitoba.ca" in theDict["hostname"] ) and ( int (theDict["ID"] ) >= 0) and ( int (theDict["ID"] ) <= ROOT_ID)  ):
					valid = True
					if( (theDict.has_key("hops") ) and ( (theDict.has_key("query") ) ) ):
						if( (int (theDict["hops"] ) <= -1) or (int (theDict["query"]) < 0) or (  (int (theDict["query"]) > ROOT_ID))  ):
							valid = False
					print "Valid Input, Answering message........"
		else:
			return valid
	except:
		print "Invalid Message........"

	return valid
		

#######################################
##	MAIN
#########################
if __name__ == "__main__":
	# Initiate a normal node with predecessor as root and successor as empty and node ID as 0
	mainNode = node( ROOT_HOSTNAME, ROOT_PORT, 0, MY_HOSTNAME, MY_PORT, '', 0, MY_ID)

	try:
		mainSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	except socket.error, msg:
		print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
		sys.exit()
	try:
		mainNode.nodeHostName = socket.getfqdn()
		mainSocket.bind((mainNode.nodeHostName, mainNode.nodePort))
	except socket.error , msg:
		print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
		sys.exit()
	
	jsonDict = {"cmd": "pred?",
				"port": MY_PORT,
				"ID": MY_ID,
				"hostname":socket.getfqdn()
	}

	jsonDict1 = {"cmd": "find",
				"port": MY_PORT,
				"ID": MY_ID,
				"hostname": "",
				"hops": 0,
				"query":65535
	}


	# # Sending JOIN request to root node
	joinhandle = nodejoin(ROOT_HOSTNAME, ROOT_PORT, MY_HOSTNAME, MY_PORT, mainNode)

	print"Finallyyyyyyy.... \n"

	while 1:
		#LISTEN.......
		jsonRecvdDict = mainNode.listensocket()

		if not validateDictionary( jsonRecvdDict ):
			print ".........waiting........."
		elif (jsonRecvdDict["cmd"] == "pred?" and not JOIN_STATE):
			print "_____________________________"
			print "I got a Pred? Request \n"
			sendersHostname = jsonRecvdDict["hostname"]
			sendersPort = jsonRecvdDict["port"]
			sendersAddr = ( sendersHostname, sendersPort )

			myPredDict = {'me': 
			{'hostname': socket.getfqdn(), 
			'port': MY_PORT, 
			'ID': mainNode.nodeID
			}, 
			'cmd': 'myPred', 
			'thePred': {'hostname': mainNode.predHostName, 
			'port': mainNode.predPort, 
			'ID': mainNode.predID}
			}
			myPredFile = json.dumps( myPredDict )

			print "Senders Address: ", (sendersAddr)
			print myPredDict
			mainSocket.sendto( myPredFile, sendersAddr )

		elif (jsonRecvdDict["cmd"] == "setPred" and not JOIN_STATE):
			print "_____________________________"
			print "I got a setPred Request \n"
			predUpdate(jsonRecvdDict,mainNode,mainSocket)
			currentDT = datetime.datetime.now()
			
			print "My predecessor has changed and it is now --", ( mainNode.predHostName, mainNode.predPort ), " \n"
			print "TimeStamp: ", (str(currentDT)), "\n"
			mainNode.printInfo()

		elif (jsonRecvdDict["cmd"] == "find" and not JOIN_STATE):
			print "_____________________________"
			print "I got a find Request \n"
			sendersHostname = jsonRecvdDict["hostname"]
			sendersPort = jsonRecvdDict["port"]
			sendersAddr = ( sendersHostname, sendersPort )

			#Check that all this keys exist
			try:
				query = int( jsonRecvdDict["query"] )
				hops = int( jsonRecvdDict["hops"] )
				initialID = int( jsonRecvdDict["ID"] )
				initialHostName = jsonRecvdDict["hostname"]
				initialPort = int( jsonRecvdDict["port"] )

				if ( mainNode.nodeID >= query ): 
					jsonOwnerDict = {"cmd": "owner",
					"ID": MY_ID,
					"hostname": socket.getfqdn(),
					"hops": hops,
					"query": query
					}
					jsonOwnerFile = json.dumps( jsonOwnerDict )

					print "I am the Owner Sending Info\n"
					mainSocket.sendto( jsonOwnerFile, sendersAddr )
				else:
					hops += 1
					jsonFindDict = {"cmd": "find",
					"port": initialPort,
					"ID": initialID,
					"hostname": initialHostName,
					"hops": hops,
					"query": query
					}
					print "Sending Message Off to my Successor", ( mainNode.succHostName, mainNode.succPort)
					findRequest( mainNode.succHostName, mainNode.succPort, jsonFindDict)
			except:
				print "Some Input Is invalid, Something went wrong"
		else:
			print ".........waiting........."