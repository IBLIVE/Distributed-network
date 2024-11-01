import random
import socket
import json
import csv
import math
import sys
import time
import threading


def is_valid_port(used_port):
    try:
        port_num = int(used_port)
        return 26500 < port_num <= 26999
    except ValueError:
        return False


def firstPrimeNumber(length):
    prime = length+1
    if prime == 5:
        return 7
    if prime == 7:
        return 11
    flag = True
    while flag:
        flag = False
        for i in range(3, 1 + int(math.sqrt(prime))):
            if (prime % i) == 0:
                prime += 2
                flag = True
                break
    return prime


class Peer:
    def __init__(self, manager_ip, manager_port):
        self.m_port = None
        self.p_port = None
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.peer_host = socket.gethostbyname(socket.gethostname())
        self.manager_socket.setblocking(False)
        self.peer_socket.setblocking(False)
        self.name = ""
        self.n = None
        self.id = None
        self.next_tuple = None
        self.events = dict()
        self.events_count = 0
        self.isRegistered = False
        self.s = None
        self.addr = None
        self.year = None
        self.wait_for_id = False
        self.items = ["event id", "state", "year", "month name", "event type", "cz type", "cz name", "injuries direct",
                      "injuries indirect", "deaths direct", "deaths indirect", "damage property", "damage crops",
                      "tor f scale"]

    def start(self):
        while True:
            if not self.isRegistered:
                while True:
                    print("Enter the command: ")
                    command = input().split()
                    flag = True
                    mport = int(command[3])
                    pport = int(command[4])
                    while flag:
                        if is_valid_port(mport) and is_valid_port(pport):
                            flag = False
                        else:
                            print("Invalid ports. Enter again: ")
                            mport = int(input("mport: "))
                            pport = int(input("pport: "))
                    if command[0] == "register":
                        self.register(command[1], command[2], int(command[3]), int(command[4]))
                        while True:
                            try:
                                manager_data, manager_addrs = self.manager_socket.recvfrom(1024)
                                message = json.loads(manager_data.decode())
                                res = self.handle_message(message, manager_addrs)
                                if res == 0:
                                    self.isRegistered = True
                                    break
                                else:
                                    print("Registering didn't work")
                            except socket.error:
                                pass
                        break
                    else:
                        print("Must register first.")
            else:
                def handle_user_input():
                    while True:
                        if self.wait_for_id:
                            continue
                        print("Enter the command: ")
                        command = input().split()
                        if command[0] == "setup-dht":
                            if len(command) != 4:
                                print("Invalid command format.")
                            else:
                                name = command[1]
                                n = int(command[2])
                                year = command[3]
                                message = {
                                    'command': 'setup-dht',
                                    'leader_name': name,
                                    'n': n,
                                    'year': year
                                }
                                self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                                # time.sleep(1)
                        elif command[0] == "dht-complete":
                            name = command[1]
                            message = {
                                'command': 'dht-complete',
                                'leader_name': name,
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                        elif command[0] == "query-dht":
                            name = command[1]
                            message = {
                                'command': 'query-dht',
                                'name': name,
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                            self.wait_for_id = True
                        elif command[0] == "show_table":
                            print(self.s)
                            print(self.events)
                            print(len(self.events))
                        elif command[0] == "leave-dht":
                            name = command[1]
                            message = {
                                'command': 'leave-dht',
                                'name': name,
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                        elif command[0] == "join-dht":
                            name = command[1]
                            message = {
                                'command': 'join-dht',
                                'name': name,
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                        elif command[0] == "dht-rebuilt":
                            pass
                        elif command[0] == "deregister":
                            name = command[1]
                            message = {
                                'command': "deregister",
                                'name' : name
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                        elif command[0] == "teardown-dht":
                            name = command[1]
                            message = {
                                'command': "teardown-dht",
                                'name': name
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                        elif command[0] == "teardown-complete":
                            name = command[1]
                            message = {
                                'command': "teardown-complete",
                                'name': name
                            }
                            self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")
                        elif command[0] == "right_neighbour":
                            print(self.next_tuple)
                        elif command[0] == "terminate":
                            quit()
                        else:
                            print("Invalid command.")

                user_input_thread = threading.Thread(target=handle_user_input, daemon=True)
                user_input_thread.start()

                while True:
                    try:
                        manager_data, manager_addrs = self.manager_socket.recvfrom(1024)
                        message = json.loads(manager_data.decode())
                        self.handle_message(message, manager_addrs)
                    except socket.error:
                        pass

                    try:
                        peer_data, peer_addr = self.peer_socket.recvfrom(1024)
                        message = json.loads(peer_data.decode())
                        self.handle_message(message, peer_addr)
                    except socket.error:
                        pass

    def register(self, name, ip, manager_port, peer_port):
        self.peer_socket.bind((self.peer_host, peer_port))
        self.manager_socket.bind((self.peer_host, manager_port))
        self.p_port = peer_port
        self.m_port = manager_port
        self.addr = ip
        while True:
            if len(name) <= 15:
                self.name = name
                break
            else:
                print("The length of a name should be <= 15 characters long")
                print("Enter the peer name (<= 15 characters): ")
                self.name = input().strip()

        message = {
            'command': 'register',
            'peerName': self.name,
            'ipAddr': ip,
            'mPort': self.m_port,
            'pPort': self.p_port
        }
        self.send_message(message, (self.manager_ip, int(self.manager_port)), "manager")

    def handle_message(self, message, addr):
        response = message.get('response')
        match response[0]:
            case "register":
                if response[1] == "SUCCESS":
                    print("Registration successful!")
                    return 0
                elif response[1] == "FAILURE":
                    print("Registration failed.")
                    return 1
                else:
                    print(f"Received unknown response: {response[1]}")
                    return 1
            case "setup-dht":
                if response[1] == "SUCCESS":
                    print("constructing DHT: ")
                    self.dht_construct(response[2], response[3])
                elif response[1] == "FAILURE":
                    print("DHT setup failed.")
                else:
                    print(f"Received unknown response: {response[1]}")
            case "dht-complete":
                if response[1] == "SUCCESS":
                    print("DHT was set up")
                else:
                    print("DHT setup was not completed.")
            case "set_id":
                self.set_id(message.get('id'), message.get('n'), message.get('peer_list'), message.get('year'))
            case "store":
                self.store(message.get('event'), message.get('id'), message.get('pos'), message.get('table_size'))
            case "query-dht":
                if response[1] == "SUCCESS":
                    print("Enter the event id")
                    event_id = int(input().strip())
                    nodes = []
                    message = {'response': ('find-event', event_id), 'querying_peer': (self.addr, self.p_port),
                               'id_seq': [], 'peer-name': response[2][0], 'I': nodes, 'random': False, 'rand_id': None}
                    self.send_message(message, (response[2][1], response[2][2]), "peer")
                else:
                    print("Wrong query.")
            case "find-event":
                if message.get('random'):
                    if message.get('rand_id') == self.id:
                        I = message.get('I')
                        I = set(I)
                        I.discard(self.id)
                        I = list(I)
                        id_seq = message.get('id_seq')
                        id_seq.append(self.id)
                        peer_name = message.get('peer-name')
                        querying_addr = message.get('querying_peer')
                        result = self.find_event(response[1], querying_addr, id_seq, I)
                        if result[0] == "SUCCESS":
                            to_que = {'response': ('event_found', "SUCCESS"), 'id_seq': result[2], 'event': result[1]}
                            self.send_message(to_que, querying_addr, "peer")
                        elif result[0] == "NOT FOUND":
                            to_que = {'response': ('find-event', response[1]), 'querying_peer': querying_addr,
                                       'id_seq': result[1], 'peer-name': peer_name, 'I': I, 'random': True,
                                       'rand_id': result[2]}
                            self.send_message(to_que, (self.next_tuple[1], self.next_tuple[2]), "peer")
                        else:
                            to_que = {'response': ('event_found', "FAILURE"), 'id_seq': result[1],
                                      'event': None, 'event_id': response[1]}
                            self.send_message(to_que, querying_addr, "peer")
                    else:
                        to_que = message
                        self.send_message(to_que, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    I = {i for i in range(self.n)}
                    I.discard(self.id)
                    I = list(I)
                    id_seq = message.get('id_seq')
                    id_seq.append(self.id)
                    peer_name = message.get('peer-name')
                    querying_addr = message.get('querying_peer')
                    result = self.find_event(response[1], querying_addr, id_seq, I)
                    if result[0] == "SUCCESS":
                        to_que = {'response': ('event_found', "SUCCESS"), 'id_seq': result[2], 'event': result[1]}
                        self.send_message(to_que, querying_addr, "peer")
                    elif result[0] == "NOT FOUND":
                        message = {'response': ('find-event', response[1]), 'querying_peer': querying_addr,
                                   'id_seq': result[1], 'peer-name': peer_name, 'I': I, 'random': True, 'rand_id': result[2]}
                        self.send_message(message, (self.next_tuple[1], self.next_tuple[2]), "peer")
                    else:
                        to_que = {'response': ('event_found', "FAILURE"), 'event_id': response[1],
                                  'id_seq': result[1], 'event': None}
                        self.send_message(to_que, querying_addr, "peer")
            case "event_found":
                if response[1] == "SUCCESS":
                    print(f"id_seq: {message.get('id_seq')}")
                    event = message.get('event')
                    ans = ""
                    for i in range(len(self.items)):
                        ans += self.items[i] + ": " + event[i] + "\n"
                    print(f"event: {ans}")
                else:
                    print(f"Storm event {message.get('event_id')} not found in the DHT.")
                self.wait_for_id = False
            case "leave-dht":
                if response[1] == "SUCCESS":
                    print("Leaving the dht")
                    to_teardown = {'response': ('teardown-dht-gen',), 'initial_peer': self.id}
                    self.send_message(to_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    print(f"An error occurred. Either the dht does not exist or the node is not a part of a dht.")
            case "teardown-dht-gen":
                if message.get('initial_peer') == self.id:
                    n = self.n
                    self.teardown_dht_gen()
                    to_reset_id = {'response': ('reset-id',), 'initial_peer': self.name, 'id': -1,
                                   'n': n-1, 'leader_addr': self.next_tuple}
                    self.send_message(to_reset_id, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    self.teardown_dht_gen()
                    to_teardown = {'response': ('teardown-dht-gen',), 'initial_peer': message.get('initial_peer')}
                    self.send_message(to_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
            case "reset-id":
                print("resetting the id")
                if message.get('initial_peer') == self.name:
                    to_rebuild = {'response': ('rebuild-dht',)}
                    self.send_message(to_rebuild, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    self.id = message.get('id')+1
                    self.n = message.get('n')
                    temp_tuple = tuple(self.next_tuple)
                    if self.next_tuple[0] == message.get('initial_peer'):
                        self.next_tuple = message.get('leader_addr')
                    to_reset_id = {'response': ('reset-id',), 'initial_peer': message.get('initial_peer'),
                                   'id': self.id, 'n': message.get('n'), 'leader_addr': message.get('leader_addr')}
                    self.send_message(to_reset_id, (temp_tuple[1], temp_tuple[2]), "peer")
            case "rebuild-dht":
                self.rebuild_dht()
                # print(addr)
                self.send_message({'response': ('dht-rebuild-complete',)}, addr, "peer")
            case "dht-rebuild-complete":
                print("DHT rebuild is complete")
                self.send_message({'command': "dht-rebuilt", 'leader_name': self.next_tuple[0], 'peer_name': self.name},
                                  (self.manager_ip, int(self.manager_port)), "manager")
            case "dht-rebuilt":
                if response[1] == "SUCCESS":
                    print("DHT rebuild was successful")
                else:
                    print("Something went wrong")
            case "join-dht":
                if response[1] == "SUCCESS":
                    print("Joining the dht")
                    self.next_tuple = response[2]
                    self.teardown_dht_gen()
                    trigger_name = self.next_tuple[0]
                    to_teardown = {'response': ('teardown-dht-join',), 'trigger_name': trigger_name,
                                   'new_node': (self.name, self.addr, self.p_port), 'id': 0, 'added': 0}
                    self.send_message(to_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    print(f"An error occurred. Either the dht does not exist or the node is not a part of a dht.")
            case "teardown-dht-join":
                if message.get('added') == 1:
                    self.n = message.get('n')
                    self.id = message.get('id')
                    to_rebuild = {'response': ('rebuild-dht',)}
                    self.send_message(to_rebuild, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    temp_n = self.n
                    self.teardown_dht_gen()
                    self.id = message.get('id')
                    self.n = temp_n + 1
                    if self.next_tuple[0] == message.get('trigger_name'):
                        self.next_tuple = message.get('new_node')
                        to_teardown = {'response': ('teardown-dht-join',), 'trigger_name': message.get('trigger_name'),
                                       'new_node': (self.name, self.addr, self.p_port), 'id': self.id+1, 'added': 1, 'n': self.n}
                        self.send_message(to_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
                    else:
                        to_teardown = {'response': ('teardown-dht-join',), 'trigger_name': message.get('trigger_name'),
                                       'new_node': message.get('new_node'), 'id': self.id+1, 'added': 0}
                        self.send_message(to_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
            case "deregister":
                if response[1] == "SUCCESS":
                    print('Successfully deregistered')
                else:
                    print('Failed to deregister')
            case "teardown-dht":
                if response[1] == "SUCCESS":
                    print('Starting teardown')
                    start_teardown = {'response': ('teardown-dht-start',), 'trigger_name': self.name}
                    self.send_message(start_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
                else:
                    print('Failed to teardown')
            case "teardown-dht-start":
                self.teardown_dht_gen()
                if self.name == message.get('trigger_name'):
                    print("Waiting for teardown-complete")
                else:
                    start_teardown = {'response': ('teardown-dht-start',), 'trigger_name': message.get('trigger_name')}
                    self.send_message(start_teardown, (self.next_tuple[1], self.next_tuple[2]), "peer")
            case "teardown-complete":
                if response[1] == "SUCCESS":
                    print('Teardown completed')
                else:
                    print('Failed to teardown')
            case _:
                print(f"Received unknown response: {response}")

    def set_id(self, i, n, ring_tuple, year):
        print("setting id")
        self.year = year
        self.id = i
        self.n = n
        num = (i+1) % n
        self.next_tuple = ring_tuple[num]

    def store(self, i, peer_id, pos, s):
        self.s = s
        if peer_id == self.id:
            self.events[pos] = i
        else:
            message = {'response': ('store',),
                       'event': i,
                       'id': peer_id,
                       'pos': pos,
                       'table_size': s,
                       }
            self.send_message(message, (self.next_tuple[1], self.next_tuple[2]), "peer")

    def send_message(self, message, addr, dest):
        serialized_message = json.dumps(message)
        ip = addr[0]
        port = addr[1]
        if dest == "manager":
            self.manager_socket.sendto(serialized_message.encode(), (ip, port))
        else:
            self.peer_socket.sendto(serialized_message.encode(), (ip, port))

    def dht_construct(self, peerList, yyyy):
        self.year = yyyy
        n = len(peerList)
        print(f"Building a topology of size {n}, year {yyyy}")
        length = 0
        nodes_record = list()
        self.events = dict()
        self.set_id(0, n, peerList, yyyy)
        for i in range(1, n):
            message = {'response': ("set_id",),
                       'id': i,
                       'peer_list': peerList,
                       'n': n,
                       'year': yyyy}
            self.send_message(message, (peerList[i][1], peerList[i][2]), "peer")

        datafile = f"details-{yyyy}.csv"

        try:
            with open(datafile, 'r', newline='', encoding='utf-8') as datafile:
                datareader = csv.reader(datafile)
                header = next(datareader)
                for _ in datareader:
                    length += 1

                s = firstPrimeNumber(int(2 * length))
                self.s = s
                nodes_record = dict()
                for i in range(self.n):
                    nodes_record[i] = set()
                datafile.seek(0)
                datareader = csv.reader(datafile)
                header = next(datareader)
                for i in datareader:
                    pos = int(i[0]) % s
                    peer_id = pos % len(peerList)
                    nodes_record[peer_id].add(pos)
                    if peer_id == 0:
                        self.events[pos] = i
                    else:
                        message = {'response': ('store',),
                                   'event': i,
                                   'id': peer_id,
                                   'pos': pos,
                                   'table_size': s,
                                   }
                        self.send_message(message, (self.next_tuple[1], self.next_tuple[2]), "peer")

        except FileNotFoundError:
            print("File not found.")
        except Exception as e:
            print(f"Error handling the file: {e}")

        for key, value in nodes_record.items():
            print(f"node {key}: {len(value)} events")
        print("Enter the command (dht-complete needed): ")

    def find_event(self, event_id, query_addr, id_seq, I):
        pos = event_id % self.s
        peer_id = pos % self.n
        if peer_id == self.id:
            if event_id == int(self.events[pos][0]):
                return ("SUCCESS", self.events[pos], id_seq)
            else:
                return ("FAILURE", id_seq)
        else:
            if len(I) == 0:
                return ("FAILURE", id_seq)
            else:
                next_node = random.choice(tuple(I))
                return ("NOT FOUND", id_seq, next_node)

    def teardown_dht_gen(self):
        self.n = 0
        self.events.clear()
        self.id = None

    def rebuild_dht(self):
        datafile = f"details-{self.year}.csv"
        length = 0
        self.events = dict()
        try:
            with open(datafile, 'r', newline='', encoding='utf-8') as datafile:
                datareader = csv.reader(datafile)
                header = next(datareader)
                for _ in datareader:
                    length += 1
                s = firstPrimeNumber(int(2 * length))
                self.s = s
                print(s)
                nodes_record = [0] * self.n
                nodes_record = dict()
                for i in range(self.n):
                    nodes_record[i] = set()
                datafile.seek(0)
                datareader = csv.reader(datafile)
                header = next(datareader)
                for i in datareader:
                    pos = int(i[0]) % s
                    peer_id = pos % self.n
                    nodes_record[peer_id].add(pos)
                    if peer_id == 0:
                        self.events[pos] = i
                    else:
                        message = {'response': ('store',),
                                   'event': i,
                                   'id': peer_id,
                                   'pos': pos,
                                   'table_size': s,
                                   }
                        self.send_message(message, (self.next_tuple[1], self.next_tuple[2]), "peer")
        except FileNotFoundError:
            print("File not found.")
        except Exception as e:
            print(f"Error handling the file: {e}")


print("Enter the manager's IP (IPv4): ")
manager_addr = input()
print("Enter the manager's port: ")
port = int(input())
dht_peer = Peer(manager_addr, port)
dht_peer.start()