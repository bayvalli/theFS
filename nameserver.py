import rpyc
import uuid
import threading
import math
import random
import ConfigParser
import signal
import pickle
import sys
import os
from pprint import pprint

from rpyc.utils.server import ThreadedServer

def int_handler(signal, frame):
    pickle.dump((NameServerService.exposed_NameServer.file_table,
                NameServerService.exposed_NameServer.block_mapping),
                open('fs.img', 'wb'))
    sys.exit(0)

def set_conf():
    conf = ConfigParser.ConfigParser()
    conf.readfp(open('dfs.conf'))
    NameServerService.exposed_NameServer.block_size = int(conf.get('nameServer', 'block_size'))
    NameServerService.exposed_NameServer.replication_factor = int(conf.get('nameServer', 'replication_factor'))
    storageList = conf.get('nameServer', 'storageList').split(',')

    for s in storageList:
        id, addr, port = s.split(":")
        NameServerService.exposed_NameServer.storageList[id]=(addr, port)
    
    if os.path.isfile('fs.img'):
        NameServerService.exposed_NameServer.file_table,
        NameServerService.exposed_NameServer.block_mapping = pickle.load(open('fs.img', 'rb'))

class NameServerService(rpyc.Service):
    class exposed_NameServer():
        file_table = {}
        block_mapping = {}
        storageList = {}

        block_size = 0
        replication_factor = 0

        def exposed_get(self, fname):
            mapping = self.file_table[fname]
            return mapping

        def exposed_put(self, fname, size):
            if self.exists(fname):
                pass

            self.file_table[fname] = []

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(fname, num_blocks)
            return blocks

        def exposed_get_file_table_entry(self, fname):
            if fname in self.file_table:
                return self.file_table[fname]
            else:
                return None
        
        def exposed_get_file_table(self):
            return self.file_table

        def exposed_get_block_size(self):
            return self.block_size

        def exposed_get_storage_list(self):
            return self.storageList

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size)/self.block_size))
        
        def exists(self, fname):
            return fname in self.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                nodes_ids = random.sample(self.storageList.keys(), self.replication_factor)
                blocks.append((block_uuid, nodes_ids))

                self.file_table[dest].append((block_uuid, nodes_ids))

            return blocks


if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(NameServerService, port = 5353)
    t.start()