# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from gzip import GzipFile
from StringIO import StringIO
from os import SEEK_SET, SEEK_END

# boto's s3.Key and botocore's StreamingBody do not support the seek()
# operation, which we need in order to pass the stream along to a
# GzipFile for gzip decompression. However, the implementation of
# GzipFile only uses seek() in combination with tell() on its fileobj
# to determine whether EOF has been reached when parsing a new
# member. Since in our case we know:
# - there is only one member per fileobj
# - it does not begin at EOF
# - the only remaining data after decompression will be the final 8 bytes of
#   gzip member verification metadata
# we can create:
# - a modified GzipFile that does not need to use seek() to perform the
#   final CRC and size verification checks
# - a wrapper around StreamingBody that ensures the first member passes the
#   not-EOF check without needing to actually perform a seek()
# Combining these two shims gives us a way to transparently perform streaming
# gzip decompression of s3 objects.

# Refer to the implementation of GzipFile for more details.


class StreamingGzipFile(GzipFile):
    def __init__(self, **args):
        super(StreamingGzipFile, self).__init__(**args)

    def _read_eof(self):
        remainder = self.decompress.unused_data + self.fileobj.read()
        assert(len(remainder) == 8)
        self.fileobj.close()
        self.fileobj = StringIO(remainder)
        super(StreamingGzipFile, self)._read_eof()


class GzipStreamingBody:
    def __init__(self, sb):
        self.sb = sb
        self.eof = False

    def tell(self):
        return int(self.eof)

    def close(self):
        return self.sb.close()

    def read(self, n=-1):
        return self.sb.read(n)

    def seek(self, offset, whence=SEEK_SET):
        if whence == SEEK_END:
            self.eof = True


def streaming_gzip_wrapper(body):
    return StreamingGzipFile(fileobj=GzipStreamingBody(body))
