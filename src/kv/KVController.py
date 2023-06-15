import os
import socketserver
import configparser
import socket
import logging
import logging.config
import threading
import time
import datetime
from pymemcache.client import base


from src import variables
from src.kv.kv import KVStore

flag_var = "flag"
exp_var = "exp"
size_var = "size"
STORED = "STORED"
value_var = "value"
NOT_STORED = "NOT STORED"
END = "END"
ERROR = "ERROR"
VAlUE = "VALUE"


class KVController:
    def __init__(self, kv_type, kv_conn, logger):
        self.kv_type = kv_type
        self.logger = logger
        self.kvStore = None
        if self.kv_type == variables.custom:
            self.kvStore = KVStore()
        elif self.kv_type == variables.memcache:
            self.kvStore = base.Client(kv_conn)

    def request(self, msg):
        cmd = msg.split()[0]
        self.logger.debug(f"Msg to KVController : {msg}")
        if cmd == variables.set:
            resp = self.set(msg)
        if cmd == variables.get:
            resp = self.get(msg)
        return resp

    def set(self, msg):
        cmd, key, flag, exp, size, value = msg.split()
        msg = f"{cmd} {key} {flag} {exp} {size}\r\n{value}\r\n"
        flag = int(flag)
        size = int(size)
        exp = int(exp)
        self.logger.debug(f"Message to kv store from kv_controller {msg}")
        if self.kv_type == variables.custom:
            resp = self.kvStore.request(msg)
        elif self.kv_type == variables.memcache:
            if len(value) > size:
                resp = f"CLIENT_ERROR bad data chunk\r\n{variables.error}"
            elif len(value) < size:
                resp = variables.error
            else:
                try:
                    self.logger.warn(f"Sending message to memcache - kv_controller")
                    resp = self.kvStore.set(key=key, value=value, expire=exp, flags=flag, noreply=False)
                    if resp:
                        resp = variables.stored
                    else:
                        resp = variables.not_stored
                except Exception as e:
                    self.logger.warn(f"Exception in KV_Controller - memcache:\n{e}")
                    resp = variables.error

        self.logger.debug(f"Response for set {key} {value} : {resp}")
        return resp.encode()

    def get(self, msg):
        cmd, key = msg.split()
        msg = f"{cmd} {key}\r\n"
        self.logger.debug(f"Message to kv store from kv_controller{msg}")


        if self.kv_type == variables.custom:
            resp = self.kvStore.request(msg)
        elif self.kv_type == variables.memcache:
            try:
                self.logger.warn(f"Sending message to memcache - kv_controller")
                resp = self.kvStore.get(key=key)
                self.logger.warn(f"Response from memcache - kv-controller")
                resp = resp.decode()
                if resp:
                    resp += "\r\n"
                resp += variables.end
            except Exception as e:
                self.logger.warn(f"Exception in KV_Controller - memcache:\n{e}")
                resp = variables.error
        self.logger.debug(f"Response for get {key} : {resp}")
        return resp.encode()






