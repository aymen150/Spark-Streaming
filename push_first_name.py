#!/usr/bin/env python3

import socket
import sys
from time import sleep
filename = "src/main/resources/data/insee/nat2018.csv"

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 9999        # Port to listen on (non-privileged ports are > 1023)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        with open(filename, 'r') as f1:
            print('Connected by', addr)

            for line in f1:
                sys.stdout.write(line)
                conn.sendall(line.encode('UTF-8'))
                sleep(0.000001)



