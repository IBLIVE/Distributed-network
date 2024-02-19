import socket
import threading
import json
import sys

class Manager:
    def __init__(self, port):
        self.port = port
        self.peerList = dict()  # To store information about connected peers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind(('localhost', port))
        self.server_socket.setblocking(False)
        self.nameSet = set()
        self.portSet = set()
        self.peerDB = dict()
        self.dhtleader = ""
        self.isActive = False

    def start(self):
        print(f"Used port: {self.port}")
        while True:
            try:
                data, addr = self.server_socket.recvfrom(1024)
                message = json.loads(data.decode())
                self.handle_message(message, addr)
            except socket.error:
                pass  # No data received within the timeout

    def handle_message(self, message, addr):
        print("got message")
        command = message.get('command')
        if command == 'register':
            response = self.register(message.get('peerName'), message.get('ipAddr'),
                                     message.get('mPort'), message.get('pPort'))
            self.send_message({'response': response}, addr)
        elif command == 'setup-dht':
            n = message.get('n')
            yyyy = message.get('yyyy')
            peerName = message.get('peerName')
            response, self.peerList = self.setup_dht(peerName, n, yyyy)
            self.send_message({'response': response, 'peer_list': self.peerList, 'yyyy': yyyy}, addr)
        elif command == 'dht-complete':
            leaderName = message.get('peerName')
            response = self.dht_complete(leaderName, addr)
            self.send_message({'response': response}, addr)
        else:
            print(f"Unknown command received: {command}")

    def register(self, peerName, ipAddr, mPort, pPort):
        if not (self.nameSet.isdisjoint({peerName}) and self.portSet.isdisjoint({mPort, pPort})):
            return "FAILURE"
        else:
            self.nameSet.add(peerName)
            self.portSet.update({mPort, pPort})
            self.peerDB.update({peerName: {
                'State': 'Free',
                'IPv4': ipAddr,
                'mPort': mPort,
                'pPort': pPort}})
            return "SUCCESS"

    def setup_dht(self, peerName, n, yyyy):
        peerList = list()
        if len(self.peerDB) < 3 or n < 3 or self.nameSet.isdisjoint({peerName}) or self.isActive:
            return "FAILURE", peerList
        else:
            self.isActive = True
            self.dhtleader = peerName    
            self.peerDB[peerName]['State'] = "Leader"
            peerList.append((peerName, self.peerDB[peerName]['IPv4'], self.peerDB[peerName]['pPort']))
            for i in self.peerDB.keys():
                if self.peerDB[i]['State'] == "Free":
                    self.peerDB[i]['State'] = "InDHT"
                    peerList.append((i, self.peerDB[i]['IPv4'], self.peerDB[i]['pPort']))
                if len(peerList) == n:
                    break
        return "SUCCESS", peerList

    def dht_complete(self, leaderName, addr):
        if leaderName == self.dhtleader:
            return "SUCCESS"
        else:
            return "FAILURE"

    def send_message(self, message, addr):
        serialized_message = json.dumps(message)
        self.server_socket.sendto(serialized_message.encode(), addr)

print("Enter the port of a manager: ")
dht_manager = Manager(int(input()))
dht_manager.start()