import os
import socketserver
import configparser
import socket
import logging
import logging.config
import threading
import time
import datetime

from src import variables

flag_var = "flag"
exp_var = "exp"
size_var = "size"
STORED = "STORED"
value_var = "value"
NOT_STORED = "NOT STORED"
END = "END"
ERROR = "ERROR"
VAlUE = "VALUE"


class KVStore:
    def __init__(self):
        self.kv_store = dict()

    def request(self, msg):
        cmd = msg.split()[0]
        if cmd == variables.set:
            resp = self.set(msg)
        if cmd == variables.get:
            resp = self.get(msg)
        return resp

    def get(self, msg):
        cmd, key = msg.split()
        msg = ""
        if key in self.kv_store:
            values = self.kv_store[key]
            msg = f"{VAlUE} {key} {values[flag_var]} {values[size_var]}\r\n{values[value_var]}\r\n"
            return msg
        msg += f"{END}\r\n"
        return msg

    def set(self, msg):
        try:
            print(msg.split())
            cmd, key, flag, exp, size, value = msg.split()
            if key in self.kv_store:
                self.kv_store[key][value_var] = value
                self.kv_store[key][flag_var] = flag
                self.kv_store[key][exp_var] = exp
                self.kv_store[key][size_var] = size
            else:
                values = dict()
                values[value_var] = value
                values[flag_var] = flag
                values[exp_var] = exp
                values[size_var] = size
                self.kv_store[key] = values
            msg = f"{STORED}\r\n"
            return msg
        except Exception as e:
            print(f"kv_excpetion {e}")
            msg = f"{NOT_STORED}\r\n"
            return msg




