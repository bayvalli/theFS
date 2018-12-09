import rpyc
import uuid
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/storage/"

class StorageService(rpyc.Service):
    # exposed classes & methods
    ALIAES = ["storage"]
    def on_connect(self,conn):
        print("[+] NameServer Connected {}".format(conn._channel.stream.sock.getpeername()))
    def on_disconnect(self,conn):
        print("[+] NameServer Disconnected {}".format(conn._channel.stream.sock.getpeername()))
    class exposed_Storage():
        blocks= {}
        def exposed_pull(self,uuid):
            data_addr = DATA_DIR+str(uuid)
            if not os.path.isfile(data_addr):
                print("[-] Block not found {}".format(uuid))
                return None
            with open(data_addr) as f:
                return f.read()
        def exposed_push(self,data,storages,uuid):
            with open(DATA_DIR+str(uuid),'w') as f:
                print("[+] Writing data to {}".format(DATA_DIR+str(uuid)))
                f.write(data)
            if len(storages)>0:
                print("[~] Forwarding to other storages")
                self.forward(data,storages,uuid)
        def exposed_delete(self, uuid):
            if os.path.isfile(DATA_DIR+str(uuid)):
                print("[+] Removing {}".format(DATA_DIR+str(uuid)))
                os.remove(DATA_DIR+str(uuid))
        def forward(self,data,storages,uuid):
            print "8888: forwarding to:"
            print uuid, storages
            storage = storages[0]
            storages = storages[1:]
            
            host,port = storage
            conn = rpyc.connect(host,port=port)
            storage = conn.root.Storage()
            storage.push(data,storages,uuid)

if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    t = ThreadedServer(StorageService, port = 8888)
    t.start()
