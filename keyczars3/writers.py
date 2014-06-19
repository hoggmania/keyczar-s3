# coding=utf-8

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from fs import s3fs
from keyczar import writers
from keyczar import errors


class S3Writer(writers.Writer):
    def __init__(self, location):
        self.location = location

    @classmethod
    def CreateWriter(cls, location):
        """Return an instance of this clas if it handles the location

        location must be a string with the format: s3://domain/bucket/
        (an optional folder inside the bucket can be used)

        """
        parse = urlparse(location)
        if parse.scheme == 's3':
            path = parse.path.split('/', 2)
            result = S3Writer(location=s3fs.S3FS(
                bucket=path[1], prefix=path[2] if len(path) == 3 else ''))
            return result
        return None

    def _write(self, path, data):
        with self.location.open(unicode(path), mode='w') as f:
            f.write(data)

    def WriteMetadata(self, metadata, overwrite=True):
        """Write the metadata for the key.

        """
        fname = 'meta'
        if not overwrite and self.location.exists(fname):
            raise errors.KeyczarError(u'File:%s already exists' % fname)
        self._write(fname, unicode(metadata))
        return

    def WriteKey(self, key, version_number, encrypter=None):
        """Write out the key at the given version.

        """
        key = unicode(key)
        if encrypter:
            key = encrypter.Encrypt(key) # encrypt key info before outputting
        self._write(version_number, key)
        return

    def Remove(self, version_number):
        """Remove the key for the given version.

        """
        self.location.remove(unicode(version_number))

    def Close(self):
        """Clean up this writer

        """
        # no-op
        return
