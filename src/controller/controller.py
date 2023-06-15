import os
import socketserver
import configparser
import socket
import logging
import logging.config
import threading
import time
import traceback

import datetime
import configparser
import logging
import random
from multiprocessing import Process, Lock
from queue import Queue
from src import variables
from src.kv.KVController import KVController
from collections import OrderedDict
from threading import Lock

# we need a datastructure that holds data till sent back to client or kv
# threadsafe
# sorted by requestid


def createLogHandler(job_name, log_file):
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(log_file + job_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class Peer:
    def __init__(self,address):
        self.sock = None
        self.heartbeat_time = 0
        self.status = False
        self.address = address
        self.syncing = False
        self.last_sync = 0

    def __str__(self):
        return f"{self.address} heartbeat:{self.heartbeat_time} status: {self.status} syncing: {self.syncing} last_sync: {self.last_sync} "


class CustomRequest:
    def __init__(self,msg,log_name,type, addr=None):
        self.msg = msg
        self.log_name = log_name
        self.req_id = str(time.time())
        self.acks = 0
        # true/false
        self.addr = addr
        self.status = False
        self.resp = None

        # direct/indirect
        self.type = type


class CustomRequestDict:
    def __init__(self):
        self.lock = Lock()
        self.data = {}

    def set(self, obj):
        with self.lock:
            self.data.update({obj.req_id: obj})
            self.data = dict(sorted(self.data.items(), key=lambda item: float(item[0])))

    def get(self):
        with self.lock:
            for key, val in self.data.items():
                if not val.status:
                    return key

    def remove(self, key):
        with self.lock:
            del self.data[key]


class Controller(socketserver.ThreadingMixIn, socketserver.TCPServer, Process):
    def __init__(self, server_address, RequestHandlerClass, config_path, name, kv_conn,
                 kv_type=variables.custom, consistency=variables.sequential):
        self.log_name = None
        self.server_address = server_address
        self.config_path = config_path
        self.name = name
        self.consistency = consistency
        self.processing_lock = Lock()
        self.last_heartbeat_time = time.time()
        # self.sockets = {}
        self.peers = {}
        self.completed_requests = {}
        self.requests = CustomRequestDict()
        self.re_election = False

        # Config Variables
        self.logger = None
        self.status = None
        self.heartbeat_time = None
        self.s = None
        self.config = None
        self.threshold = None

        self.load_config()

        super().__init__(server_address, RequestHandlerClass)

        self.kv_controller = KVController(kv_type=kv_type, kv_conn=kv_conn, logger=self.logger)

        self.heartbeat_send_thread = threading.Thread(target=self.send_heartbeat_members, daemon=True)
        self.heartbeat_recv_thread = threading.Thread(target=self.recv_heart_beat, daemon=True)
        if self.consistency == variables.sequential or self.consistency == variables.linearizability:
            self.check_requests_thread = threading.Thread(target=self.check_requests, daemon=True)
            self.check_requests_thread.start()

        self.heartbeat_send_thread.start()
        self.heartbeat_recv_thread.start()

    def load_config(self):
        """
        This method reads and loads the config for the server from the given config file
        :return:
        """
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        log_file = self.config.get(variables.common, variables.log_file)
        self.log_name = f"{self.name}"
        self.logger = createLogHandler(job_name=self.log_name, log_file=log_file)
        self.logger.debug(f"Config loaded for {self.name}")
        self.heartbeat_time = self.config.getint(variables.common, variables.heartbeat_time)
        self.threshold = self.config.getint(variables.common, variables.threshold)
        for i in self.config.get(variables.common,variables.peers).split(","):
            addr = (i.split(":")[0],int(i.split(":")[1]))
            self.peers[addr] = Peer(addr)
        if self.consistency == variables.sequential:
            pass

    def check_requests(self):
        while True:
            # self.logger.debug("Inside check request method")
            # process requests in the order that they were received
            while self.requests.get() is not None:
                obj = self.requests.data[self.requests.get()]
                try:
                    if obj.type == variables.direct:
                        if obj.msg.split()[0] == variables.set:
                            # check consistency
                            if self.consistency == variables.sequential or self.consistency == variables.linearizability:
                                addr = self.server_address[0] + ":" + str(self.server_address[1])
                                for i in self.peers:
                                    if not self.peers[i].status:
                                        continue
                                        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                        # sock.connect(self.peers[i].address)
                                        # self.peers[i].sock = sock

                                    self.logger.warn(f"Sending this indirect message: {obj.msg} {addr} {str(obj.req_id)} \r\n")
                                    self.peers[i].sock.sendall((obj.msg + " " + addr + " " + str(obj.req_id)+"\r\n").encode())

                                k = 0
                                while k < 3:
                                    self.logger.warn(f"Current acks {obj.acks} and required {len([i for i in self.peers if self.peers[i].status])}")
                                    if obj.acks >= len([i for i in self.peers if self.peers[i].status]):
                                        obj.status = True
                                    else:
                                        time.sleep(1)
                                    k += 1
                                if not obj.status:
                                    raise Exception("Insufficient acks")
                        elif obj.msg.split()[0] == variables.get:
                            # check consistency
                            if self.consistency == variables.linearizability:
                                for i in self.peers:
                                    print(i)
                                    if self.peers[i].status:
                                        self.peers[i].sock.sendall((obj.msg + " " +str(""+self.server_address[0]+":"+str(self.server_address[1]))+' '+ str(obj.req_id)+"\r\n").encode())
                                k = 0
                                while k < 3:
                                    if obj.acks >= len([i for i in self.peers if self.peers[i].status]):
                                        obj.status = True
                                    else:
                                        time.sleep(1)
                                    k += 1
                                print(obj.acks)
                                if not obj.status:
                                    raise Exception("Insufficient acks")

                    obj.resp = self.kv_controller.request(obj.msg)
                    obj.status = True
                    self.logger.warn(f"OBJ Status : {obj.status} and response : {obj.resp}")
                    if obj.type == variables.indirect:
                        self.peers[obj.addr].sock.sendall(f"{variables.response} {obj.req_id} {obj.resp.decode()} \r\n".encode())
                        # self.requests.remove(obj.req_id)

                except Exception as e:
                    self.logger.error(e)

            time.sleep(self.heartbeat_time)

    def recv_heart_beat(self):
        while True:
            for i in self.peers:
                current_time = time.time()
                if current_time - self.peers[i].heartbeat_time > self.heartbeat_time * self.threshold:
                    self.peers[i].status = False
                    self.peers[i].sock = None
                    self.peers[i].syncing = False
                else:
                    self.peers[i].status = True
            time.sleep(self.heartbeat_time)

    def send_msg(self, addr, msg):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((addr[0], int(addr[1])))
            s.sendall(msg.encode())
            self.logger.debug("Message sent from send_msg")

    def send_heartbeat_members(self):
        reset_sync = 0
        while True:
            if reset_sync==3:
                for i in self.peers:
                    self.peers[i].syncing = False
                reset_sync = 0
            for j, i in self.peers.items():

                if self.server_address != i.address:
                    try:
                        # check if socket conn exists, else create socket
                        msg = f"{variables.heartbeat} {self.server_address[0]} {self.server_address[1]}\r\n"
                        self.logger.debug(f"sending {msg}")
                        if not i.sock or not i.status:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.connect(i.address)
                            i.sock = sock
                        i.sock.sendall(msg.encode())
                        if self.consistency == variables.eventual and not i.syncing:
                            msg = f"{variables.sync} {variables.request} {self.server_address[0]} {self.server_address[1]} {i.last_sync}\r\n"
                            self.logger.warn(f"Sync requesting msg: {msg}")
                            i.sock.sendall(msg.encode())
                            i.syncing = True
                            self.logger.warn(f"Sync set to true for {i}")

                    except Exception as e:
                        self.logger.warn(f"{e} at {i.address}")
                        traceback.print_exc()
                        i.status = False
                        self.logger.warn(f"{i} status set to False")

            time.sleep(self.heartbeat_time)
            reset_sync += 1


class RequestHandler(socketserver.StreamRequestHandler):

    def __init__(self, request, client_address, server):
        self.client_address = client_address
        super().__init__(request, client_address, server)

    def handle(self):
        while True:
            try:
                org_msg = self.rfile.readline().strip().decode()
                if not org_msg:
                    return
                msg = org_msg.split()
                self.server.logger.debug(f"msg split {str(msg)}")
                if msg[0] == variables.response:
                    self.server.logger.debug(f"Got response for req_id {msg[1]} and length is {len(msg)}")
                    req_id = msg[1]
                    if len(msg) >= 3 and msg[2] != variables.error:
                        self.server.logger.debug(f"Inside the if block for response {msg[2]}")
                        self.server.requests.data[req_id].acks += 1
                        self.server.logger.warn(f"Acks incremented to {self.server.requests.data[req_id].acks} for"
                                                f"message {self.server.requests.data[req_id].msg}")
                elif msg[0] == variables.sync:
                    address = (msg[2], int(msg[3]))
                    time_stamp = float(msg[4])
                    if msg[1] == variables.request:
                        self.server.logger.warn(f"Inside sync req. len is {len(msg)} msg: {msg}")
                        msgs = []
                        for i in self.server.completed_requests:
                            if self.server.completed_requests[i][0] > time_stamp:
                                msgs.append(f"{self.server.completed_requests[i][0]} {i} {self.server.completed_requests[i][1]}")
                        sync_response = f"{variables.sync} {variables.response}"
                        my_addr = f"{self.server.server_address[0]} {self.server.server_address[1]}"
                        for i in msgs:
                            self.server.peers[address].sock.sendall(f"{sync_response} {my_addr} {i}\r\n".encode())
                        self.server.peers[address].sock.sendall(
                            f"{sync_response} {my_addr} {time.time()} {variables.complete}\r\n".encode())

                    elif msg[1] == variables.response:
                        self.server.logger.warn(f"Inside sync resp. len is {len(msg)} msg: {msg}")
                        if len(msg) == 6 and msg[5] == variables.complete:
                            self.server.logger.warn(f"sync set to false for {address}")
                            self.server.peers[address].syncing = False
                        else:
                            self.server.logger.warn("Inside sync resp handling set mesg")
                            key = msg[5]
                            req_msg = " ".join(msg[6:])
                            self.server.peers[address].last_sync = time_stamp
                            if key in self.server.completed_requests and self.server.completed_requests[key][0] > time_stamp:
                                self.server.logger.warn(f"for key {key} current - {self.server.completed_requests[key][0]} new - {time_stamp} ")
                            else:
                                self.server.kv_controller.request(req_msg)
                                self.server.completed_requests[key] = (time_stamp, req_msg)

                elif msg[0] == variables.heartbeat:
                    address = (msg[1], int(msg[2]))
                    last_heartbeat_time = time.time()
                    self.server.peers[address].heartbeat_time = last_heartbeat_time
                    self.server.peers[address].status = True

                # elif msg[0] == variables.add:
                #     if self.server.role == variables.leader:
                #         self.server.members += [(msg[1], int(msg[2]))]
                #         self.server.logger.warn("Added")
                #         self.server.logger.warn(self.server.members)
                #         members = ",".join([str(i[0]) + ":" + str(i[1]) for i in self.server.members])
                #         self.request.sendall(f"{variables.added} {msg[0]} {msg[1]} {members}".encode())
                #
                # elif msg[0] == variables.added:
                #     if (msg[1], int(msg[2])) == self.server.server_address:
                #         self.server.members = msg[3].split(",")
                #         self.server.logger.warn("Added a new member/follower")
                #         self.server.logger.warn(self.server.members)


                elif msg[0] == variables.get or msg[0] == variables.set:
                    if self.server.consistency == variables.eventual:
                        resp = self.server.kv_controller.request(org_msg)
                        self.request.sendall(resp)
                        if msg[0] == variables.set:
                            self.server.completed_requests[msg[1]] = (time.time(), org_msg)
                    elif msg[0] == variables.get and self.server.consistency == variables.sequential:
                        resp = self.server.kv_controller.request(org_msg)
                        self.request.sendall(resp)

                    else:
                        self.server.logger.debug(f"Get request message {org_msg}")
                        try:
                            self.server.logger.debug("in try")
                            t = float(msg[-1])
                            req_msg = " ".join(msg[:-2])
                            self.server.logger.debug(f"message is indirect with req_id {str(t)} and msg : {req_msg}")
                            type_ofrequest = variables.indirect
                            if msg[-1] in self.server.requests.data:
                                req = self.server.requests.data[msg[-1]]
                            else:
                                self.server.logger.warn(f'addr is {msg[-2]}')
                                addr = (msg[-2].split(":")[0], int(msg[-2].split(":")[1]))
                                self.server.logger.warn(f"addr is {addr} and msg is {req_msg}")
                                req = CustomRequest(req_msg, self.server.log_name, type_ofrequest, addr)
                                req.req_id = msg[-1]
                        except Exception as e:
                            print(e)
                            self.server.logger.warn(f"{e} request is {variables.direct}")
                            type_ofrequest = variables.direct
                            req = CustomRequest(org_msg, self.server.log_name, type_ofrequest)

                        self.server.requests.set(req)
                        # self.server.requests_fulfilled[req_id] = None
                        while not self.server.requests.data[req.req_id].status or \
                                self.server.requests.data[req.req_id].resp is None:
                            # time.sleep(1)
                            pass

                        if type_ofrequest == variables.direct:
                            self.server.logger.warn('direct msg to client')
                            self.request.sendall(self.server.requests.data[req.req_id].resp)
                            self.server.logger.warn(f"message sent {self.server.requests.data[req.req_id].resp}")
                        self.server.requests.remove(req.req_id)


                elif msg[0] == variables.who:
                    self.request.sendall(f"I am {str(self.server.server_address)}".encode())

                elif msg[0] == variables.show:
                    peers = [str(i) for k, i in self.server.peers.items()]
                    rqs = [i for i in list(self.server.requests.data)]
                    rqs = [i for i in list(self.server.completed_requests)]
                    msg = f"I am : {str(self.server.server_address)} \nPeers : {str(peers)} \nRequests : {str(rqs)}"
                    self.request.sendall(msg.encode())
                else:
                    self.request.sendall(org_msg.encode())
            except Exception as e:
                print(e)
                self.server.logger.warn(e)
                traceback.print_exc()
