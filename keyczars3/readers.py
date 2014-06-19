# coding=utf-8

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from fs import s3fs
from keyczar import readers


class S3Reader(readers.Reader):
    def __init__(self, location):
        self.location = location

    @classmethod
    def CreateReader(cls, location):
        parse = urlparse(location)
        if parse.scheme == 's3':
            path = parse.path.split('/', 2)
            result = S3Reader(location=s3fs.S3FS(
                bucket=path[1], prefix=path[2] if len(path) == 3 else ''))
            return result
        return None

    def _read(self, path):
        with self.location.open(path, mode='r') as f:
            return f.read()

    def GetMetadata(self):
        return self._read('meta')

    def GetKey(self, version_number):
        return self._read(unicode(version_number))

    def Close(self):
        # no-op
        return
