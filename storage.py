import rpyc
import uuid
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/storage/"

class StorageService(rpyc.Service):
    # exposed classes & methods
    class exposed_Storage():
        blocks= {}
        def exposed_pull(self,uuid):
            data_addr = DATA_DIR+str(uuid)
            if not os.path.isfile(data_addr):
                return None
            with open(data_addr) as f:
                return f.read()
        def exposed_push(self,data,storages,uuid):
            with open(DATA_DIR+str(uuid),'w') as f:
                f.write(data)
            if len(storages)>0:
                # forward data to other storages
                self.forward(data,storages,uuid)
        def forward(self,data,storages,uuid):
            print "8888: forwarding to:"
            print uuid, storages
            storage = storages[0]
            storages = storages[1:]
            
            host,port = storage
            conn = rpyc.connect(host,port=port)
            storage = conn.root.Storage()
            storage.push(data,storages,uuid)
        def delete_data(self,uuid):
            pass

if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    t = ThreadedServer(StorageService, port = 8888)
    t.start()
