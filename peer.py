import socket
import json
import sys
import csv
import math


def is_valid_port(used_port):
    try:
        port_num = int(used_port)
        return 26500 < port_num <= 26999
    except ValueError:
        return False


def firstPrimeNumber(length):
    prime = length+1
    flag = True
    while flag:
        flag = False
        for i in range(3, 1 + int(math.sqrt(prime))):
            if (prime % i) == 0:
                prime += 2
                flag = True
    return prime


class Peer:
    def __init__(self, manager_ip, manager_port, pport, mport):
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.manager_socket.setblocking(False)
        self.peer_socket.setblocking(False)
        self.peer_socket.bind(('10.10.1.2', pport))
        self.manager_socket.bind(('10.10.1.2', mport))
        self.p_port = pport
        print(type(self.p_port))
        self.m_port = mport
        print(type(self.m_port))
        self.name = ""
        self.id = None
        self.nexttuple = None
        self.events = list()
        self.events_count = 0

    def start(self):
        self.register()

        while True:
            try:
                peer_data, peer_addr = self.peer_socket.recvfrom(1024)
                message = json.loads(peer_data.decode())
                self.handle_message(message, peer_addr)

                manager_data, manager_addr = self.manager_socket.recvfrom(1024)
                message = json.loads(manager_data.decode())
                self.handle_message(message, manager_addr)
            except socket.error:
                pass

    def register(self):
        while True:
            print("Enter the peer name (<= 15 characters): ")
            self.name = input().strip()
            if len(self.name) <= 15:
                break
            else:
                print("Length of the name should be <= 15 characters long")

        while True:
            if is_valid_port(self.p_port):
                break
            else:
                print("Invalid port format. Please enter a valid port.")

        while True:
            if is_valid_port(self.m_port):
                break
            else:
                print("Invalid port format. Please enter a valid port.")

        message = {
            'command': 'register',
            'peerName': self.name,
            'ipAddr': socket.gethostbyname(socket.gethostname()),  # Get the IPv4 address of the peer
            'mPort': self.m_port,
            'pPort': self.p_port  # Use the same port for simplicity, adjust as needed
        }

        self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")

    def handle_message(self, message, addr):
        while True:
            response = message.get('response')
            if response == "SUCCESS":
                peerList = message.get('peer_list')

                if peerList is not None:
                    print("Received peer list:", peerList)
                    self.set_id(0)
                    self.nexttuple = peerList[1]
                    self.construct_dht(peerList, message.get('yyyy'))
                    return

                print("Issue another command? (Y/N)")
                if input().strip().upper() == "Y":
                    print("Enter the next command (setup-dht if DHT still was not set up): ")
                    commandName = input().strip().lower()

                    if commandName == "setup-dht" and peerList is None:
                        print("Enter the topology size: ")
                        n = int(input().strip())
                        print("Enter the year: ")
                        yyyy = input().strip()
                        message = {'peerName': self.name,
                                   'command': commandName,
                                   'n': n,
                                   'yyyy': yyyy}
                        self.send_message(message, addr, "manager")
                        return

                    elif commandName == "dht-complete":
                        message = {'peerName': self.name,
                                   'command': commandName}
                        self.send_message(message, addr, "manager")
                        return

                    else:
                        print("Invalid command. More commands will be added later.")

                elif input().strip().upper() == "N":
                    return

                else:
                    print("Incorrect format")

            elif response == 'set_id':
                self.set_id(message.get('id'))
                self.nexttuple = message.get('nexttuple')
                return

            elif response == "FAILURE":
                print("FAILURE encountered.")
                return

            elif response == 'store':
                if message.get('id') == self.id:
                    self.events[self.events_count] = message.get('event')
                    self.events_count += 1
                else:
                    self.send_message(message, (self.nexttuple[1], self.nexttuple[2]), "peer")
                return

            else:
                print("Incorrect response")
                return

    def send_message(self, message, addr, dest):
        serialized_message = json.dumps(message)
        ip = '10.10.1.1'
        port = 26501
        if dest == "manager":
            #self.manager_socket.sendto(message.encode(), (str(addr[0]), int(addr[1])))
            self.manager_socket.sendto(serialized_message.encode(), (ip, port))
        else:
            self.peer_socket.sendto(serialized_message.encode(), addr)

    def construct_dht(self, peerList, yyyy):
        n = len(peerList)
        print(f"Building a topology of size {n}, year {yyyy}")
        length = 0

        for i in range(1, n):
            message = {'response': 'set_id',
                       'id': i,
                       'nexttuple': peerList[(i+1) % n],
                       'yyyy': yyyy}
            self.send_message(message, (peerList[i][1], peerList[i][2]), "peer")

        datafile = f"data-{yyyy}.csv"

        try:
            with open(datafile, 'r', newline='', encoding='utf-8') as datafile:
                datareader = csv.reader(datafile)
                header = next(datareader)
                for _ in datareader:
                    length += 1
        except FileNotFoundError:
            print("File not found.")
        except Exception as e:
            print(f"Error handling the file: {e}")

        s = firstPrimeNumber(int(2*length))
        self.events = [None]*s

        try:
            with open(datafile, 'r', newline='', encoding='utf-8') as datafile:
                datareader = csv.reader(datafile)
                header = next(datareader)
                for i in datareader:
                    pos = int(i[0]) % s
                    peer_id = pos % len(peerList)
                    message = {'response': 'store',
                               'event': i,
                               'id': peer_id}
                    self.send_message(message, (self.nexttuple[1], self.nexttuple[2]), "peer")
        except FileNotFoundError:
            print("File not found.")
        except Exception as e:
            print(f"Error handling the file: {e}")

    def set_id(self, peer_id):
        self.id = peer_id


if __name__ == "__peer__":
    if len(sys.argv) != 3:
        print("Incorrect IPv4 and port format")
        sys.exit(1)
    port = sys.argv[2]
    manager_addr = sys.argv[1]
    print("Enter the port for interaction with the manager: ")
    pport = int(input())
    print("Enter the port for interaction with peers: ")
    mport = int(input())
    dht_peer = Peer(manager_addr, port, pport, mport)
    dht_peer.start()

print("Enter an IP (IPv4): ")
manager_addr = input()
print("Enter the manager's port: ")
port = int(input())
print("Enter the port for interaction with the manager: ")
pport = int(input())
print("Enter the port for interaction with peers: ")
mport = int(input())
dht_peer = Peer(manager_addr, port, pport, mport)
dht_peer.start()