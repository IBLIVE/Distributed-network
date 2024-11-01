import socket

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_host = '0.0.0.0'
print("Enter a port number: ")
server_port = int(input().strip())
server_socket.bind((server_host, server_port))

# Listen for incoming connections
server_socket.listen(5)
print(f'Server listening on {server_host}:{server_port}')

while True:
    # Wait for a connection
    print('Waiting for a connection...')
    client_socket, addr = server_socket.accept()
    print(f'Got connection from {addr}')

    # Receive the data in small chunks and print it
    data = client_socket.recv(1024)
    print(f'Received: {data.decode()}')

    # Send a response back to the client
    response = 'Hello from the server!'
    client_socket.sendall(response.encode())

    # Clean up the connection
    client_socket.close()