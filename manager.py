import socket
import json
import random


def is_valid_port(used_port):
    try:
        port_num = int(used_port)
        return 26500 < port_num <= 26999
    except ValueError:
        return False


class Peer:
    def __init__(self, name, ip_addr, mport, pport):
        self.name = name
        self.ip_addr = ip_addr
        self.mport = mport
        self.pport = pport
        self.state = "Free"


class Manager:
    def __init__(self, port):
        self.port = port
        self.peerList = list()  # To store information about connected peers
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_host = socket.gethostbyname(socket.gethostname())
        self.server_socket.bind((self.server_host, self.port))
        self.server_socket.setblocking(False)
        self.nameSet = set()
        self.portSet = set()
        self.peerDB = dict()
        self.dhtleader = ""
        self.n = None
        self.isActive = False
        self.waitfordht = False
        self.waitforrebuild = False
        self.dht_completed = False
        self.waitforteardown = False
        self.name_stored = None

    def start(self):
        print(f"Used port: {self.port}, used host: {self.server_host}")
        while True:
            try:
                data, addr = self.server_socket.recvfrom(1024)
                message = json.loads(data.decode())
                self.handle_message(message, addr)
            except socket.error:
                pass  # No data received within the timeout

    def handle_message(self, message, addr):
        print("Incoming message")
        if self.waitforteardown:
            if message.get('command') != "teardown-complete":
                response = (message.get('command'), "FAILURE")
                self.send_message({'response': response}, addr)
                return
            else:
                pass
        if self.waitfordht:
            if message.get('command') != "dht-complete":
                response = (message.get('command'), "FAILURE")
                self.send_message({'response': response}, addr)
                return
            else:
                response = self.dht_complete(message.get('leader_name'))
                if response == "SUCCESS":
                    self.waitfordht = False
                    response = ("dht-complete", "SUCCESS")
                    self.dht_completed = True
                else:
                    response = ("dht-complete", "FAILURE")
                self.send_message({'response': response}, addr)
                return
        if self.waitforrebuild:
            if message.get('command') != "dht-rebuilt":
                response = (message.get('command'), "FAILURE")
                self.send_message({'response': response}, addr)
                return
            else:
                response = self.dht_rebuilt(message.get('peer_name'), message.get('leader_name'))
                if response == "SUCCESS":
                    self.waitforrebuild = False
                    response = ("dht-rebuilt", "SUCCESS")
                    self.dht_completed = True
                else:
                    response = ("dht-rebuilt", "FAILURE")
                self.send_message({'response': response}, addr)
                return
        command = message.get('command')
        match command:
            case "register":
                response = self.register(message.get('peerName'), message.get('ipAddr'),
                                         message.get('mPort'), message.get('pPort'))
                self.send_message({'response': response}, addr)
            case "setup-dht":
                response = self.setup_dht(message.get('leader_name'), message.get('n'), message.get('year'))
                if response[1] == "SUCCESS":
                    self.waitfordht = True
                self.send_message({'response': response}, addr)
            # case "dht-complete":
            #     pass
            case "query-dht":
                response = self.query_dht(message.get('name'))
                self.send_message({'response': response}, addr)
            case "leave-dht":
                response = self.leave_dht(message.get('name'))
                self.send_message({'response': response}, addr)
            case "join-dht":
                response = self.join_dht(message.get('name'))
                self.send_message({'response': response, 'n': self.n}, addr)
            case "dht-rebuilt":
                pass
            case "deregister":
                name = message.get('name')
                # if not self.nameSet.isdisjoint(name) and self.peerDB[name]['State'] == "Free":
                if self.peerDB[name]['State'] == "Free":
                    self.nameSet.discard(name)
                    self.portSet.discard(self.peerDB[name]['m_port'])
                    self.portSet.discard(self.peerDB[name]['p_port'])
                    del self.peerDB[name]
                    self.send_message({'response': ("deregister", "SUCCESS")}, addr)
                else:
                    self.send_message({'response': ("deregister", "FAILURE",)}, addr)
            case "teardown-dht":
                name = message.get('name')
                ans = None
                if self.dhtleader != name:
                    ans = ("teardown-dht", "FAILURE")
                else:
                    self.waitforteardown = True
                    ans = ("teardown-dht", "SUCCESS")
                self.send_message({'response': ans}, addr)
            case "teardown-complete":
                name = message.get('name')
                if self.dhtleader != name:
                    ans = ("teardown-complete", "FAILURE")
                else:
                    self.waitforteardown = False
                    for i in self.nameSet:
                        if self.peerDB[i]['State'] != "Free":
                            self.peerDB[i]['State'] = "Free"
                    ans = ("teardown-complete", "SUCCESS")
                self.send_message({'response': ans}, addr)
            case _:
                print(f"Unknown command received: {command}")

    def register(self, peerName, ipAddr, mPort, pPort):
        if not (self.nameSet.isdisjoint({peerName}) and self.portSet.isdisjoint({mPort, pPort})):
            print("got failure")
            return ("register", "FAILURE")
        else:
            print("got success")
            self.nameSet.add(peerName)
            self.portSet.update({mPort, pPort})
            newPeer = Peer(peerName, ipAddr, mPort, pPort)
            self.peerDB[peerName] = {'IPv4': ipAddr, 'm_port': mPort, 'p_port': pPort, 'State': "Free"}
            self.peerList.append(newPeer)
            return ("register", "SUCCESS")

    def setup_dht(self, leader_name, n, yyyy):
        peerList = list()
        if len(self.peerDB) < 3 or n < 3 or self.nameSet.isdisjoint({leader_name}) or self.isActive:
            return ("setup-dht", "FAILURE", peerList, yyyy)
        else:
            self.n = n
            self.isActive = True
            self.dhtleader = leader_name
            self.peerDB[leader_name]['State'] = "Leader"
            peerList.append((leader_name, self.peerDB[leader_name]['IPv4'], self.peerDB[leader_name]['p_port']))
            while len(peerList) < n:
                next_peer = list(self.peerDB.keys())[random.randint(0, len(self.peerDB)-1)]
                if self.peerDB[next_peer]['State'] != "Free":
                    continue
                else:
                    self.peerDB[next_peer]['State'] = "InDHT"
                    peerList.append((next_peer, self.peerDB[next_peer]['IPv4'], self.peerDB[next_peer]['p_port']))

        return ("setup-dht", "SUCCESS", peerList, yyyy)

    def dht_complete(self, leaderName):
        if leaderName == self.dhtleader:
            return "SUCCESS"
        else:
            return "FAILURE"

    def query_dht(self, name):
        if self.dht_completed and name in self.nameSet and self.peerDB[name]['State'] == "Free":
            while True:
                query_peer = list(self.peerDB.keys())[random.randint(0, len(self.peerDB)-1)]
                if self.peerDB[query_peer]['State'] == "Free":
                # if self.peerDB[query_peer]['State'] != "InDHT":
                    continue
                else:
                    query_peer_tuple = (query_peer, self.peerDB[query_peer]['IPv4'], self.peerDB[query_peer]['p_port'])
                    return ("query-dht", "SUCCESS", query_peer_tuple)
        else:
            return ("query-dht", "FAILURE")
        return 0

    def leave_dht(self, name):
        if self.dht_completed and self.peerDB[name]['State'] != 'Free':
            self.name_stored = name
            self.waitforrebuild = True
            return ("leave-dht", "SUCCESS")
        else:
            return ("leave-dht", "FAILURE")

    def join_dht(self, name):
        if self.dht_completed and self.peerDB[name]['State'] == 'Free':
            while True:
                query_peer = list(self.peerDB.keys())[random.randint(0, len(self.peerDB)-1)]
                if self.peerDB[query_peer]['State'] == "Free":
                    continue
                else:
                    self.name_stored = name
                    self.waitforrebuild = True
                    query_peer_tuple = (query_peer, self.peerDB[query_peer]['IPv4'], self.peerDB[query_peer]['p_port'])
                    return ("join-dht", "SUCCESS", query_peer_tuple)
        else:
            return ("join-dht", "FAILURE")

    def dht_rebuilt(self, peer_name, leader_name):
        if self.name_stored != peer_name:
            return "FAILURE"
        else:
            if self.peerDB[self.name_stored]['State'] == 'Free':
                self.peerDB[self.name_stored]['State'] = 'InDHT'
            else:
                self.peerDB[self.name_stored]['State'] = 'Free'
            self.peerDB[self.dhtleader]['State'] = 'InDHT'
            self.dhtleader = leader_name
            self.peerDB[self.dhtleader]['State'] = 'Leader'
            print(f"The new leader: {self.dhtleader}")
            return "SUCCESS"
        pass

    def send_message(self, message, addr):
        print("sending the message")
        print(f"ip {addr[0]}")
        print(f"port {addr[1]}")
        serialized_message = json.dumps(message)
        self.server_socket.sendto(serialized_message.encode(), addr)


print("Enter the port of a manager: ")
flag = True
port = 0
while flag:
    port = int(input())
    if is_valid_port(port):
        flag = False
    else:
        print("Invalid port. Enter again: ")
dht_manager = Manager(port)
dht_manager.start()
