import socket
import time
from _socket import SHUT_WR
import configparser

from src import variables


class Client:
    """
    Client for sequencer
    """

    def __del__(self):
        if self.socket is not None:
            self.socket.shutdown(SHUT_WR)
            self.socket.close()

    def __init__(self, host: str, port: int):
        """
        :param host: host arg passed from the driver code
        :param port: port arg passed from the driver code
        """
        self.socket = None
        self.host = host
        self.port = port

    def __str__(self):
        return f"{self.host}:{self.port}"

    def __repr__(self):
        return self.__str__()

    def connect(self):
        try:
            time.sleep(1)
            self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            address = self.socket.getsockname()[0] + " " + str(self.socket.getsockname()[1])
            while True:
                msg = input().strip()
                if msg.split()[0] == variables.set:
                    val = input().strip()
                    msg += " " + val
                msg = msg + "\r\n"
                print("from ", self.socket.getsockname(), "\nmsg:\t", msg)
                self.client_send(msg)
                while True:
                    msg = self.socket.recv(1000)
                    msg = msg.decode()
                    if msg:
                        print(msg)
                        break
                    else:
                        print("waiting")

        except Exception as e:
            print(e)

    def client_send(self, data: str):
        return self.socket.sendall(data.encode())

    def client_connect_send(self, cmd,recv = False):
        resp = ''
        with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            # s.settimeout(10)
            # print(cmd)
            s.sendall(cmd.encode())
            if recv:
                while True:
                    r = s.recv(1000)
                    resp += r.decode()
                    if r:
                        # print(resp)
                        break

                # print(resp)
        return resp