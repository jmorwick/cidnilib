from abc import abstractmethod
from collections.abc import Callable
from typing import BinaryIO
from hashlib import sha256
from typing import Protocol, runtime_checkable
from multihash import to_b58_string, from_b58_string, encode

@runtime_checkable
class HashAlgorithm(Protocol):
    def update(self, data: bytes) -> None: ...
    def digest(self) -> bytes: ...

hashers = {
    12: sha256
}

class MultiHashEncoder(HashAlgorithm):

    def __init__(self, code:int=12):
        self.hasher = hashers[code]()
        self.code = code

    def update(self, data:bytes):
        self.hasher.update(data)

    def digest(self):
        return encode(self.hasher.digest(), self.code)
    
class DataService:

    def __init__(self, 
                 encoder: Callable[[bytes],str] = to_b58_string, 
                 decoder: Callable[[str],bytes] = from_b58_string, 
                 hasher: Callable[[],HashAlgorithm] = MultiHashEncoder): 
        self.encode = encoder
        self.decode = decoder
        self.hasher = hasher

    @abstractmethod
    def know_file(self, fp:BinaryIO) -> tuple[bytes, bool]: 
        """remember given data for future retrieval"""
        pass

    @abstractmethod
    def know(self, data:bytes) -> tuple[bytes, bool]:
        """remember given data for future retrieval"""
        pass

    @abstractmethod
    def recall_binary(self, id:bytes) -> bytes:
        """retrieve data associated with id"""
        pass

    @abstractmethod
    def recall_stream(self) -> BinaryIO:
        """retrieve data associated with id"""
        pass

    def recall(self, id:str) -> bytes:
        """retrieve data associated with id"""
        return self.recall_binary(self.decode(id))

    def known_binary(self, id:bytes) -> bool:
        """determine if value is available for given id"""
        return True if self.recall_binary(id) else False

    def known(self, id:str) -> bool:
        """determine if value is available for given id"""
        return self.known_binary(self.decode(id))

