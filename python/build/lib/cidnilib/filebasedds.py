from .main import DataService, HashAlgorithm, MultiHashEncoder
from collections.abc import Callable
from typing import BinaryIO
from io import BytesIO
from pickledb import PickleDB
import os
from os.path import exists
from os import walk
from multihash import to_b58_string, from_b58_string
    
class FileBasedDataService(DataService):
    def __init__(self,
                 path: str,
                 encoder: Callable[[bytes],str] = to_b58_string, 
                 decoder: Callable[[str],bytes] = from_b58_string, 
                 hasher: Callable[[],HashAlgorithm] = MultiHashEncoder,
                 size_limit: int = 256, 
                 levels: int = 2):  
        super().__init__(encoder, decoder, hasher)
        self.path = path
        self.size_limit = size_limit
        self.levels = levels

    def resolve_path(self, id:str):
        """find file on the path that matches the id if it exists"""
        subdir = ''
        for i in range(self.levels):
            subdir += id[-i-2] + '/'
            if not exists(self.path+'/'+subdir):
                os.mkdir(self.path+'/'+subdir)
        path = self.path+'/'+subdir+id+'.bin' 
        return path

    def file_store(self, id:str, data:bytes):
        """create new file to store data in"""
        path = self.resolve_path(id)
        if not exists(path):
            fp = open(path, 'wb')
            fp.write(data)
            fp.close()

    def resolve_db(self, id:str) -> PickleDB:
        """find pickledb on the path that matches the name and generate if it doesn't exist"""
        subdir = ''
        for i in range(self.levels):
            subdir += id[-i-2] + '/'
            if not exists(self.path+'/'+subdir):
                os.mkdir(self.path+'/'+subdir)
        return PickleDB(self.path+'/'+subdir+'pickle.db')  

    def know_file(self, fp:BinaryIO):
        m = self.hasher()
        while True:
            data = fp.read(104857600)
            if not data:
                break
            m.update(data)
            print(".", end="", flush=True)
        digest = m.digest()
        id = self.encode(digest)
        path = self.resolve_path(id)
        if not exists(path):
            fp.seek(0)
            fpo = open(path, 'wb')
            while True:
                data = fp.read(104857600)
                if not data:
                    break
                fpo.write(data)
                print(".", end="", flush=True)
            fpo.close()
            return id, True
        else:
            return id, False

    def known(self, id:str):
        db = self.resolve_db(id)
        return db[id] or exists(self.resolve_path(id))

    def know(self, data:bytes):
        m = self.hasher()
        m.update(data)
        id = self.encode(m.digest())
        if len(data) < self.size_limit:
            # store in pickledb
            db = self.resolve_db(id)
            if not db.get(id):
                db[id] = self.encode(data)
                db.save()
                return id, True
            else:
                return id, False
        else:
            # store in file
            if not exists(self.resolve_path(id)):
                self.file_store(id, data)
                return id, True
            else:
                return id, False

    def recall(self, id:str):
        """retrieve data associated with name"""
        # TODO: handle collisions
        path = self.resolve_path(id)
        if exists(path):
            fp = open(path, 'rb')
            data = fp.read()
            fp.close()
            return data
        else:
            db = self.resolve_db(id)
            data = db[id]
            if not data: return None
            return self.decode(data)

    def recall_stream(self, id:str):
        """retrieve data associated with name"""
        # TODO: handle collisions
        path = self.resolve_path(id)
        if exists(path):
            fp = open(path, 'rb')
            return fp
        else:
            db = self.resolve_db(id)
            return BytesIO(self.decode(db[id]))

    def recall_binary(self, id:bytes):
        return self.recall(self.encode(id))
