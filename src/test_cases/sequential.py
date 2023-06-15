import time

import configparser

import unittest
from src import variables as v
from src.client.client import Client


def generate_set_cmd(val):
    return f"set k 0 0 {len(val)} {val}"


class TestSequentialMethods(unittest.TestCase):
    def setUp(self):
        self.config = configparser.ConfigParser()
        self.config.read(v.config_path)
        self.peers = []
        for i in self.config.get(v.common, v.peers).split(","):
            self.peers.append(Client(i.split(":")[0], int(i.split(":")[1])))
        self.get_cmd = "get k\r\n"
        self.set_cmds = ["ok", "try", "some", "value"]
        self.show_cmd = "show"

    def tearDown(self):
        self.peers = []

    def test_get_set_same_node_peer1(self):
        self.setUp()
        print()
        print(f"Sending {generate_set_cmd(self.set_cmds[0])} to {self.peers[0]}")
        self.peers[0].client_connect_send(generate_set_cmd(self.set_cmds[0]))
        print(f"Sending {self.get_cmd.strip()} to {self.peers[0]}")
        resp = self.peers[0].client_connect_send(self.get_cmd, recv=True)
        print(f"Response - {resp.strip()}")
        self.assertIn(self.set_cmds[0], resp)
        print(self.id(), self.defaultTestResult().wasSuccessful())

    def test_get_set_same_node_peer2(self):
        self.setUp()
        print()
        print(f"Sending {generate_set_cmd(self.set_cmds[1])} to {self.peers[1]}")
        self.peers[1].client_connect_send(generate_set_cmd(self.set_cmds[2]))
        print(f"Sending {self.get_cmd.strip()} to {self.peers[0]}")
        resp = self.peers[1].client_connect_send(self.get_cmd, recv=True)
        print(f"Response - {resp.strip()}")
        self.assertIn(self.set_cmds[1], resp)
        print(self.id(), self.defaultTestResult().wasSuccessful())

    def test_get_set_same_node_peer3(self):
        self.setUp()
        print()
        print(f"Sending {generate_set_cmd(self.set_cmds[2])} to {self.peers[2]}")
        self.peers[2].client_connect_send(generate_set_cmd(self.set_cmds[2]))
        print(f"Sending {self.get_cmd.strip()} to {self.peers[2]}")
        resp = self.peers[2].client_connect_send(self.get_cmd, recv=True)
        print(f"Response - {resp.strip()}")
        self.assertIn(self.set_cmds[2], resp)
        print(self.id(), self.defaultTestResult().wasSuccessful())

    def test_get_set_different_nodes1_2(self):
        self.setUp()
        print()
        print(f"Sending {generate_set_cmd(self.set_cmds[0])} to {self.peers[0]}")
        self.peers[0].client_connect_send(generate_set_cmd(self.set_cmds[0]))
        print(f"Sending {self.get_cmd.strip()} to {self.peers[1]}")
        while True:
            self.setUp()
            resp = self.peers[1].client_connect_send(self.get_cmd, recv=True)
            print(f"Response - {resp.strip()}")
            if self.set_cmds[0] in resp:
                break
            else:
                print(f"Response - {resp.strip()}")
                time.sleep(2)
        self.assertIn(self.set_cmds[0], resp)
        print(self.id(), self.defaultTestResult().wasSuccessful())

    def test_get_set_different_nodes1_3(self):
        self.setUp()
        print()
        print(f"Sending {generate_set_cmd(self.set_cmds[0])} to {self.peers[0]}")
        self.peers[0].client_connect_send(generate_set_cmd(self.set_cmds[0]))
        print(f"Sending {self.get_cmd.strip()} to {self.peers[1]}")
        while True:
            self.setUp()
            resp = self.peers[2].client_connect_send(self.get_cmd, recv=True)
            print(f"Response - {resp.strip()}")
            if self.set_cmds[0] in resp:
                break
            else:
                print(f"Response - {resp.strip()}")
                time.sleep(2)
        self.assertIn(self.set_cmds[0], resp)
        print(self.id(), self.defaultTestResult().wasSuccessful())


if __name__ == '__main__':
    unittest.main()
