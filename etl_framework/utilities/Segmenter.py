"""utility to segment list into smaller chunks"""

class Segmenter(object):
    """class to segment data into smaller chunks"""

    def __init__(self, chunk_size):
        """creates instance of Segmenter"""

        self.chunk_size = chunk_size

    def yield_segments(self, data):
        """yields equally segmented chunks"""

        for offset in range(0, len(data), self.chunk_size):
            yield data[offset:offset+self.chunk_size]
