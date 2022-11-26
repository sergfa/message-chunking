import cProfile
import io
import pstats
import unittest
from pstats import SortKey

from chunk.chunker import Base64ChunkProducer, EscapedJsonChunkProducer, ChunkProducer


class TestChunkProducer(unittest.TestCase):
    def setUp(self) -> None:
        with open("tests/samples/message.json", "r") as message_file:
            self.message = message_file.read()
        self.pr: cProfile.Profile = cProfile.Profile()

    def tearDown(self) -> None:
        self.pr.disable()
        s = io.StringIO()
        sort_by = SortKey.CUMULATIVE
        ps = pstats.Stats(self.pr, stream=s).sort_stats(sort_by)
        ps.print_stats()
        print(s.getvalue())

    def test_base64_producer(self):
        self.pr.enable()
        message_from_chunks = TestChunkProducer.run_producer(
            self.message, Base64ChunkProducer
        )
        self.pr.disable()
        self.assertEqual(self.message, message_from_chunks)

    def test_escaped_json_producer(self):
        self.pr.enable()
        message_from_chunks = TestChunkProducer.run_producer(
            self.message, EscapedJsonChunkProducer
        )
        self.pr.disable()
        self.assertEqual(self.message, message_from_chunks)

    @staticmethod
    def run_producer(message: str, chunk_producer_cls: ChunkProducer):
        chunks = []
        chunk_producer: ChunkProducer = chunk_producer_cls(message, message_id="1")
        for chunk in chunk_producer.to_chunks():
            chunks.append(chunk)
        return chunk_producer.from_chunks(chunks)


if __name__ == "__main__":
    unittest.main()
