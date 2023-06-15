"""
This file serves as a driver code for the memcached-lite
"""
import argparse
import configparser
import socketserver
import socket

from src import variables
from src.client.client import Client
from src.controller.controller import Controller, RequestHandler

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--port", type=int, help="connect to port <port>")
    parser.add_argument("-i", "--host", type=str, help="connect to host <host>")
    parser.add_argument("-c", "--config", type=str, help="when included, loads config from tha path")
    parser.add_argument("-pr", "--priority", type=int, help="priority of the controller")
    parser.add_argument("-m", "--member", help="when included, creates controller as follower", action="store_true")
    parser.add_argument("-l", "--leader", help="when included, creates controller as leader", action="store_true")
    parser.add_argument("-cli", "--client", help="when included, creates as client", action="store_true")
    parser.add_argument("-kvh", "--kvhost", type=str, help="the KV host address")
    parser.add_argument("-kvp", "--kvport", type=str, help="the KV port address")
    parser.add_argument("-kv", "--kvtype", type=str, help="the KV store to use")
    parser.add_argument("-cons", "--consistency", type=str, help="the consistency to be used")

    args = parser.parse_args()

    print(args)

    if args.client:
        c = Client(host=args.host, port=args.port)
        c.connect()
    else:
        if args.config:
            config = configparser.ConfigParser()
            try:
                config.read(args.config)
            except configparser.Error as e:
                print("Config file unreadable or does not exists", str(e))
                exit()
            config_path = args.config
        else:
            print("leader/follower running with default config file")
            config_path = variables.config_path
            config = configparser.ConfigParser()
            try:
                config.read(config_path)
            except configparser.Error as e:
                print("Config file not found!", e)
                exit()
        if args.consistency:
            consistency = args.consistency
        else:
            consistency = variables.sequential

        # if args.leader:
        #     # setup and start leader server
        #     host = config.get(variables.common, variables.leader_host)
        #     port = config.getint(variables.common, variables.leader_port)
        #
        #     # create connection to connect with KV store
        #     if args.kvtype == variables.memcache:
        #         kv_type = args.kvtype
        #         if args.kvhost and args.kvport:
        #             address = (args.kvhost, int(args.kvport))
        #     elif args.kvtype == variables.custom:
        #         kv_type = args.kvtype
        #         if args.kvhost and args.kvport:
        #             address = (args.kvhost, int(args.kvport))
        #
        #     # pass KV to leader
        #     leader_server = Controller((host, port), RequestHandler, config_path, role="leader",
        #                                priority=args.priority, kv_conn=address, kv_type=kv_type, consistency=consistency)
        #     leader_server.serve_forever()
        #
        # if args.member:
        #     # setup and start follower server

        host, port = '0.0.0.0', 0
        if args.host:
            host = args.host
        if args.port:
            port = args.port

        if args.kvtype == variables.memcache:
            kv_type = args.kvtype
            if args.kvhost and args.kvport:
                address = (args.kvhost, int(args.kvport))
        elif args.kvtype == variables.custom:
            kv_type = args.kvtype
            if args.kvhost and args.kvport:
                address = (args.kvhost, int(args.kvport))

        # pass KV to member
        peer_server = Controller((host, port), RequestHandler, config_path, name="peer_"+str(port), kv_conn=address, kv_type=kv_type, consistency=consistency)
        peer_server.serve_forever()
