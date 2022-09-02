#!/usr/bin/env python3

import ipaddress
import threading
import time
from zlib import crc32

import zmq

NETWORK_NAME = "toobee"
NETWORK_ID = crc32(NETWORK_NAME.encode("utf-8"))

BIND_PORT = 42000
BIND_SOCKET = f"tcp://*:{BIND_PORT}"

SERVER_WORKERS = 2
SERVER_WORKERS_SOCKET = "inproc://s"

CLIENT_WORKERS = 2
CLIENT_WORKERS_SOCKET = "inproc://c"

peers = [
    (
        ipaddress.IPv4Address("127.0.0.1"),
        BIND_PORT,
    ),
]


def server(context: zmq.Context):
    socket = context.socket(zmq.REP)
    socket.connect(SERVER_WORKERS_SOCKET)

    while True:
        message = socket.recv()
        print(f"server recv: [ {message} ]")
        time.sleep(1)
        socket.send(b"pong")


def client(context: zmq.Context):
    socket = context.socket(zmq.REP)
    socket.connect(CLIENT_WORKERS_SOCKET)

    peer_socket = context.socket(zmq.REQ)

    while True:
        for peer in peers:
            peer_socket.connect(f"tcp://{peer[0]}:{peer[1]}")
            peer_socket.send(b"ping")
            message = peer_socket.recv()
            print(f"client recv [{message}]")


def main():
    context = zmq.Context()

    # Listen for incoming requests
    clients = context.socket(zmq.ROUTER)
    clients.bind(BIND_SOCKET)

    # Socket to talk to workers
    workers = context.socket(zmq.DEALER)
    workers.bind(SERVER_WORKERS_SOCKET)

    # Start server workers
    for i in range(SERVER_WORKERS):
        thread = threading.Thread(target=server, args=(context,))
        thread.daemon = True
        thread.start()

    # Start client workers
    for i in range(CLIENT_WORKERS):
        thread = threading.Thread(target=client, args=(context,))
        thread.daemon = True
        thread.start()

    # Connect listener to workers
    zmq.proxy(clients, workers)


if __name__ == "__main__":
    main()
