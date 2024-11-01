import socket
import re

def is_valid_ipv4(ip):
    # Define the pattern for IPv4 address
    ipv4_pattern = r'^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$'

    # Compile the regular expression pattern
    pattern = re.compile(ipv4_pattern)

    # Check if the input string matches the pattern
    if pattern.match(ip):
        return True
    else:
        return False

# Create a TCP socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Example usage
ip_address = input("Enter an IPv4 address: ")

while True:
    if is_valid_ipv4(ip_address):
        break
    else:
        print("Invalid format.")
        ip_address = input("Enter an IPv4 address: ")

# Get local machine IP address
server_host = ip_address
server_port = int(input("Enter a server port number: "))  # Same port as used by the server

# Connect to the server
client_socket.connect((server_host, server_port))
print(f'Connected to {server_host}:{server_port}')

# Send data to the server
message = 'Hello from the client!'
client_socket.sendall(message.encode())

# Receive the response
data = client_socket.recv(1024)
print(f'Received: {data.decode()}')

# Clean up the connection
client_socket.close()