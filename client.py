import sys
import rpyc
import os
import getpass
from pprint import pprint


def usage():
    print("./client.py IPADDRESS")

def parseCMD():
    return raw_input('theFS> ').split()

def xor_crypt(data, key='awesomepassword', encode=False, decode=False):
    from itertools import izip, cycle
    import base64
    
    if decode:
        data = base64.decodestring(data)
    xored = ''.join(chr(ord(x) ^ ord(y)) for (x,y) in izip(data, cycle(key)))    
    if encode:
        return base64.encodestring(xored).strip()
    return xored

def getFileList(ns):
    file_table = ns.get_file_table()
    for entry in file_table:
        print("{}".format(entry))
    print("")

def read_from_storage(block_uuid, storage):
    addr, port = storage
    con = rpyc.connect(addr, port=port)
    ss = con.root.Storage()
    return ss.pull(block_uuid)

def getFile(ns, fname):
    file_table = ns.get_file_table_entry(fname)
    if not file_table:
        print("[-] File not found!")
        return

    passphrase = getpass.getpass("Enter a passphrase to decrypt the file: ") 
    with open(fname, "w") as f:
        for block in file_table:
            for storageServer in [ns.get_storage_list()[_] for _ in block[1]]:
                data = read_from_storage(block[0], storageServer)
                if data:
                    f.write(xor_crypt(data, passphrase, decode=True))
                    break
            else:
                print("[-] No blocks found! Possible corruption!")
                return

def send_to_storage(block_uuid, data, storageList):
    print("sending: {} -> {}".format(str(block_uuid), str(storageList)))
    storage = storageList[0]
    storages = storageList[1:]
    addr, port = storage

    con = rpyc.connect(addr, port=port)
    storage = con.root.Storage()
    storage.push(data, storages, block_uuid)

def putFile(ns, fname):
    size = os.path.getsize(fname)
    blocks = ns.put(fname, size)
    passphrase = getpass.getpass("Enter a passphrase to encrypt the file: ") 
    with open(fname) as f:
        for b in blocks:
            data = xor_crypt(f.read(ns.get_block_size()), passphrase, encode=True)
            block_uuid=b[0]
            storageList = [ns.get_storage_list()[_] for _ in b[1]]
            send_to_storage(block_uuid, data, storageList)

def delete_from_storage(block_uuid, storage):
    addr, port = storage
    con = rpyc.connect(addr, port=port)
    ss = con.root.Storage()
    return ss.delete(block_uuid)

def rmFile(ns, fname):
    file_table = ns.get_file_table_entry(fname)
    if not file_table:
        print("[-] File not found!")
        return
    
    for block in file_table:
        for storageServer in [ns.get_storage_list()[_] for _ in block[1]]:
            delete_from_storage(block[0], storageServer)
    
    ns.delete_file_entry(fname)

def processCommand(ns, parameters):
    if len(parameters) == 0:
        return
    elif parameters[0] == "quit":
        exit()
    elif parameters[0] == "list" or parameters[0] == "ls":
        getFileList(ns)
    elif parameters[0] == "get":
        getFile(ns, parameters[1])
    elif parameters[0] == "put":
        putFile(ns, parameters[1])
    elif parameters[0] == "delete" or parameters[0] == "rm":
        rmFile(ns, parameters[1])
    else:
        return
        

def main(args):
    addr, port = ("localhost", 5353)
    if len(args) > 0:
        addr = args[0]
    if len(args) > 1:
        port = int(args[1])

    con = rpyc.connect(addr, port)
    nameServer = con.root.NameServer()

    while True:
        processCommand(nameServer, parseCMD())


if __name__ == "__main__":
    main(sys.argv[1:])