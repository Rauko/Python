import sys, os, atexit, pickle, pdb
import socket, socketserver
from concurrent.futures import ThreadPoolExecutor, wait

from dictdb import DictDB

PROMT = "%s> "

peers_db = DictDB("peers.txt")

stop = False 
peers = {} 
threads = []

class Event:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    event_type = None 
    username = None 
    server_port = None 
    message = None 

class ChatPeer:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    username = None
    port = None 
    socket = None 

def create_socket(port):
    sock = socket.socket(socket.AF_INET)
    sock.connect(("localhost", port))
    return sock

def broadcast(event):
    for peer in peers.values():
        peer.socket.send(pickle.dumps(event))


def input_message_thread():
    while not stop:
        message = input()
        broadcast(Event(
            event_type="MESSAGE", username=username, message=message))

def read_data(rsock):
    CHUNK_SIZE = 16*1024
    data = b""
    chunk = rsock.recv(CHUNK_SIZE)
    return chunk #%HACK

def server_thread(sock):

    sock.listen(1)

    def process_event(event):
        if event.event_type == "REGISTER":
            port = event.server_port
            socket = create_socket(port)
            peers[port] = ChatPeer(
                username=event.username, server_port=port, socket=socket)
            print("User %s joined chat." % event.username)
        elif event.event_type == "MESSAGE":
            print(PROMT % event.username + event.message.strip())
        elif event.event_type == "LEAVE":
            peer = peers[event.server_port]
            print("User %s leaved chat." % peer.username)
            peer.socket.close()
            del peers[event.server_port]
        else:
            raise RuntimeError("Incorrect type message %s" % event.event_type)
    
    def client_listen_thread(csock):
        while not stop:
            event = pickle.loads(read_data(csock))
            process_event(event)
            if event.event_type == "LEAVE":
                csock.close()
                break

    while not stop:
        csock, addr = sock.accept()
        thread = executor.submit(client_listen_thread, csock)
        threads.append(thread)

    sock.close()

def input_username():

    used_usernames = peers_db.keys()

    while True:
        username = input("Enter your login: ")

        if username not in used_usernames:
            print("Welcome to the chat %s." % username)
            break
        else:
            print("This login %s is already in use. Chose other one." % username)

    return username

def connect_to_peers(server_port):

    for port, user in list(peers_db.items()):
        port = int(port)
        try:
            socket = create_socket(port)
            peers[port] = ChatPeer(username = user,
                port = port,
                socket = socket)

        except ConnectionError:
            del peers_db[str(port)]

    peers_db[server_port] = username

    broadcast(Event(
        event_type="REGISTER", username=username, server_port = server_port))

def shutdown(server_port):

    if str(server_port) in peers_db:
        del peers_db[str(server_port)]
		
    broadcast(Event(event_type="LEAVE", server_port=server_port))

    stop = True
    sock.close()  
    for thread in threads:
        thread.cancel()
    executor.shutdown(wait=False)

if __name__ == "__main__":

    executor = ThreadPoolExecutor(max_workers=100)
    port = None

    try:
        username = input_username()
        
        sock = socket.socket()
        sock.bind(("localhost", 0))
        server_future = executor.submit(server_thread, sock)
        port = sock.getsockname()[1]

        connect_to_peers(port)

        input_future = executor.submit(input_message_thread)

        threads.extend([server_future, input_future])
        wait([server_future, input_future])

    except (KeyboardInterrupt):
        print("Good luck.")
    finally:
        shutdown(port)
        if os.name != "nt":
            os._exit(0)