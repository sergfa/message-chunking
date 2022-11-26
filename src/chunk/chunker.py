import abc
import json
import math
import sys
from base64 import b64encode, b64decode
from typing import List, Dict, Any

# message size for Kafka should not exceed 1Mb
DDD_MAX_CHUNK_SIZE = 2**20


class ChunkProducer(abc.ABC):
    def __init__(
        self, message: str, message_id: str, chunk_size: int = DDD_MAX_CHUNK_SIZE
    ) -> None:
        self.message_id = message_id
        self.chunk_size = chunk_size
        if chunk_size <= 0:
            raise ValueError("chunk_size should be positive number")
        self.encoded_message = self.encode(message)
        self.encoded_message_length = len(self.encoded_message)
        assert self.encoded_message_length == sys.getsizeof(
            self.encoded_message
        ) - sys.getsizeof("")
        self.chunk_total = int(math.ceil(self.encoded_message_length / self.chunk_size))

    def __repr__(self):
        return (
            f"{self.__class__.__name__}: "
            f"(chunk_total: {self.chunk_total},"
            f" encoded_message_length={self.encoded_message_length})"
            f' Encoded  message size={sys.getsizeof(self.encoded_message) - sys.getsizeof("")})'
            f" chunk_size={self.chunk_size}"
        )

    @abc.abstractmethod
    def encode(self, message: str) -> str:
        pass

    @abc.abstractmethod
    def decode(self, encoded_message: str) -> str:
        pass

    def to_chunks(self) -> Dict[str, Any]:
        chunk_position = 0
        for i in range(0, self.encoded_message_length, self.chunk_size):
            chunk_data = self.encoded_message[i : i + self.chunk_size]
            assert sys.getsizeof(chunk_data) - sys.getsizeof("") <= self.chunk_size
            chunk = ChunkProducer.create_chunk(
                self.message_id, chunk_position, self.chunk_total, chunk_data
            )
            chunk_position += 1
            yield chunk

    def from_chunks(self, chunks: List[Dict[str, Any]]) -> str:
        chunks_data = [chunk["chunk_data"] for chunk in chunks]
        encoded_data = "".join(chunks_data)
        return self.decode(encoded_data)

    @staticmethod
    def create_chunk(
        message_id: str, chunk_position: int, chunk_total: int, chunk_data: str
    ) -> Dict[str, int | str]:
        return {
            "message_id": message_id,
            "chunk_position": chunk_position,
            "chunk_total": chunk_total,
            "chunk_data": chunk_data,
        }


class Base64ChunkProducer(ChunkProducer):
    def __init__(
        self,
        message: str,
        message_id: str,
        chunk_size: int = DDD_MAX_CHUNK_SIZE,
        encoding: str = "utf-8",
    ):
        self.encoding = encoding
        super().__init__(message, message_id, chunk_size)

    def encode(self, message: str) -> str:
        return b64encode(message.encode(self.encoding)).decode(self.encoding)

    def decode(self, encoded_message: str) -> str:
        return b64decode(encoded_message).decode(self.encoding)


class EscapedJsonChunkProducer(ChunkProducer):
    def encode(self, message: str) -> str:
        return json.dumps(message)

    def decode(self, encoded_message: str) -> str:
        return json.loads(encoded_message)
