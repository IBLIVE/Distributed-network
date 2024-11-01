import socket
import math


nameSet = set()
portSet = set()
peerDB = dict()
isActive = False
dhtTable = dict()


def findPrime(number):
    if number == 1:
        return 2
    if number == 2:
        return 3
    if number == 3:
        return 5
    i = 0
    flag = True
    if number % 2:
        i = number + 2
    else:
        i = number + 1
    while flag:
        flag = False
        for j in range(3, int(math.sqrt(i)), 2):
            if i % j == 0:
                flag = True
                i += 2
                break
    return i


def setid(id, ringsize, peer, ip, pport):
    dhtTable.update({id: {"ringsize": ringsize, "peerName": peer, "IPv4": ip, "pPort": pport}})
    pass


def register(peerName, ipAddr, mPort, pPort):
    if not nameSet.isdisjoint({peerName}) or not portSet.isdisjoint({mPort, pPort}):
        return "FAILURE"
    else:
        nameSet.add(peerName)
        portSet.update({mPort, pPort})
        peerDB.update({peerName: {
            "State": "Free",
            "IPv4": ipAddr,
            "mPort": mPort,
            "pPort": pPort}})
        return "SUCCESS"


def dhtsetup(peerName, n, yyyy):
    global isActive
    peerList = list()

    if len(peerDB) < 3 or n < 3 or nameSet.isdisjoint({peerName}) or isActive:
        return "FAILURE", peerList
    else:
        isActive = True
        peerDB[peerName]["State"] = "Leader"
        peerList.append((peerName, peerDB[peerName]["IPv4"], peerDB[peerName]["pPort"]))
        for i in peerDB.keys():
            if peerDB[i]["State"] == "Free":
                peerDB[i]["State"] = "InDHT"
                peerList.append((i, peerDB[i]["IPv4"], peerDB[i]["pPort"]))
            if len(peerList) == n:
                break

    return "SUCCESS", peerList


def dhtcomplete(peerName):
    pass


def dhtquery(peerName):
    pass


def dhtleave(peerName):
    pass


def dhtjoin(peerName):
    pass


def dhtrebuilt(peerName, newLeader):
    pass


def deregister(peerName):
    pass


def dhtteardown(peerName):
    pass


def teardowncomplete(peerName):
    pass


#for i in range(len(peerList)):
    #setid(i, n, peerList[(i + 1) % n][0], peerList[(i + 1) % n][1], peerList[(i + 1) % n][2])