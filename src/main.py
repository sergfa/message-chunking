import os
import sys
from pprint import pprint
from uuid import uuid4

from chunk.chunker import Base64ChunkProducer

CHUNK_SIZE = 2**10


def main() -> None:
    if len(sys.argv) < 2:
        print(f"File name is missing. Use : python main.py /path/to/message.json")
        return
    file_path = sys.argv[1]
    print(file_path)
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist!")
        return
    with open(file_path, "r") as message_file:
        message = message_file.read()
    chunk_producer = Base64ChunkProducer(
        message, message_id=str(uuid4()), chunk_size=CHUNK_SIZE
    )
    chunks = []
    for chunk in chunk_producer.to_chunks():
        pprint(chunk)
        chunks.append(chunk)

    message_from_chunks = chunk_producer.from_chunks(chunks)
    assert message_from_chunks == message


if __name__ == "__main__":
    main()
