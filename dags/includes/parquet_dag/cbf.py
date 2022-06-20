import math
import zlib


class CBF:
    def __init__(self, items, false_positive_probability):
        self.n = items
        self.m = -1 * round((self.n * math.log(false_positive_probability)) / (math.log(2)) ** 2)
        self.bit_array = [0] * self.m

    def hash_adler32(self, item):
        return zlib.adler32(item.encode()) % self.m

    def hash_crc32(self, item):
        return zlib.crc32(item.encode()) % self.m

    def add(self, item):
        ind1 = self.hash_adler32(item)
        self.bit_array[ind1] += 1
        ind2 = self.hash_crc32(item)
        self.bit_array[ind2] += 1

    def check(self, item):
        ind1 = self.hash_adler32(item)
        ind2 = self.hash_crc32(item)
        if self.bit_array[ind1] > 0 and self.bit_array[ind2] > 0:
            return True
        else:
            return False
