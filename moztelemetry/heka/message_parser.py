# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import gzip
import ssl
import struct

import boto
import snappy
import ujson as json
from cStringIO import StringIO
from google.protobuf.message import DecodeError

from .message import Message, Header


def parse_heka_message(message):
    try:
        for record, total_bytes in unpack(message):
            yield _parse_heka_record(record)

    except ssl.SSLError:
        pass  # https://github.com/boto/boto/issues/2830


def _parse_heka_record(record):
    if record.message.payload:
        result = json.loads(record.message.payload)
    else:
        result = {}
    result["meta"] = {
        # TODO: uuid, logger, severity, env_version, pid
        "Timestamp": record.message.timestamp,
        "Type":      record.message.type,
        "Hostname":  record.message.hostname,
    }

    for field in record.message.fields:
        name = field.name.split('.')
        value = field.value_string
        if field.value_type == 1:
            # TODO: handle bytes in a way that doesn't cause problems with JSON
            # value = field.value_bytes
            continue
        elif field.value_type == 2:
            value = field.value_integer
        elif field.value_type == 3:
            value = field.value_double
        elif field.value_type == 4:
            value = field.value_bool

        if len(name) == 1:  # Treat top-level meta fields as strings
            result["meta"][name[0]] = value[0] if len(value) else ""
        else:
            _add_field(result, name, value)

    return result


def _add_field(container, keys, value):
    if len(keys) == 1:
        blob = value[0] if len(value) else ""
        container[keys[0]] = _lazyjson(blob)
        return

    key = keys.pop(0)
    container[key] = container.get(key, {})
    _add_field(container[key], keys, value)


def _lazyjson(content):
    if not isinstance(content, basestring):
        raise ValueError("Argument must be a string.")

    if content.startswith("{"):
        default = {}
    elif content.startswith("["):
        default = []
    else:
        try:
            return float(content) if '.' in content or 'e' in content.lower() else int(content)
        except:
            return content

    class WrapperType(type(default)):
        pass

    def wrap(method_name):
        def _wrap(*args, **kwargs):
            if not hasattr(WrapperType, '__cache__'):
                setattr(WrapperType, '__cache__', json.loads(content))

            cached = WrapperType.__cache__
            method = getattr(cached, method_name)
            return method(*args[1:], **kwargs)

        return _wrap

    wrapper = WrapperType(default)
    for k, v in type(default).__dict__.iteritems():
        if k == "__doc__":
            continue
        else:
            setattr(WrapperType, k, wrap(k))
    return wrapper


_record_separator = 0x1e


class BacktrackableFile:
    def __init__(self, stream):
        self._stream = stream
        self._buffer = StringIO()

    def read(self, size):
        buffer_data = self._buffer.read(size)
        to_read = size - len(buffer_data)

        if to_read == 0:
            return buffer_data

        stream_data = self._stream.read(to_read)
        self._buffer.write(stream_data)

        return buffer_data + stream_data

    def close(self):
        self._buffer.close()
        if type(self._stream) == boto.s3.key.Key:
            if self._stream.resp:  # Hack! Connections are kept around otherwise!
                self._stream.resp.close()

            self._stream.close(True)
        else:
            self._stream.close()

    def backtrack(self):
        buf = self._buffer.getvalue()
        index = buf.find(chr(_record_separator), 1)

        self._buffer = StringIO()
        if index >= 0:
            self._buffer.write(buf[index:])
            self._buffer.seek(0)


class UnpackedRecord():
    def __init__(self, raw, header, message=None, error=None):
        self.raw = raw
        self.header = header
        self.message = message
        self.error = error


# Returns (bytes_skipped=int, eof_reached=bool)
def read_until_next(fin, separator=_record_separator):
    bytes_skipped = 0
    while True:
        c = fin.read(1)
        if c == '':
            return bytes_skipped, True
        elif ord(c) != separator:
            bytes_skipped += 1
        else:
            break
    return bytes_skipped, False


# Stream Framing:
#  https://hekad.readthedocs.org/en/latest/message/index.html
def read_one_record(input_stream, raw=False, verbose=False, strict=False, try_snappy=True):
    # Read 1 byte record separator (and keep reading until we get one)
    total_bytes = 0
    skipped, eof = read_until_next(input_stream, 0x1e)
    total_bytes += skipped
    if eof:
        return None, total_bytes
    else:
        # we've read one separator (plus anything we skipped)
        total_bytes += 1

    if skipped > 0:
        if strict:
            raise ValueError("Unexpected character(s) at the start of record")
        if verbose:
            print "Skipped", skipped, "bytes to find a valid separator"

    raw_record = struct.pack("<B", 0x1e)

    # Read the header length
    header_length_raw = input_stream.read(1)
    if header_length_raw == '':
        return None, total_bytes

    total_bytes += 1
    raw_record += header_length_raw

    # The "<" is to force it to read as Little-endian to match the way it's
    # written. This is the "native" way in linux too, but might as well make
    # sure we read it back the same way.
    (header_length,) = struct.unpack('<B', header_length_raw)

    header_raw = input_stream.read(header_length)
    if header_raw == '':
        return None, total_bytes
    total_bytes += header_length
    raw_record += header_raw

    header = Header()
    header.ParseFromString(header_raw)
    unit_separator = input_stream.read(1)
    total_bytes += 1
    if ord(unit_separator[0]) != 0x1f:
        error_msg = "Unexpected unit separator character at offset {}: {}".format(
            total_bytes, ord(unit_separator[0])
        )
        if strict:
            raise ValueError(error_msg)
        return UnpackedRecord(raw_record, header, error=error_msg), total_bytes
    raw_record += unit_separator

    message_raw = input_stream.read(header.message_length)

    total_bytes += header.message_length
    raw_record += message_raw

    message = None
    if not raw:
        message = Message()
        parsed_ok = False
        if try_snappy:
            try:
                message.ParseFromString(snappy.decompress(message_raw))
                parsed_ok = True
            except:
                # Wasn't snappy-compressed
                pass
        if not parsed_ok:
            # Either we didn't want to attempt snappy, or the
            # data was not snappy-encoded (or it was just bad).
            message.ParseFromString(message_raw)

    return UnpackedRecord(raw_record, header, message), total_bytes


def unpack_file(filename, **kwargs):
    fin = gzip.open(filename, "rb") if filename.endswith(".gz") else open(filename, "rb")
    return unpack(fin, **kwargs)


def unpack_string(string, **kwargs):
    return unpack(StringIO(string), **kwargs)


def unpack(fin, raw=False, verbose=False, strict=False, backtrack=False, try_snappy=True):
    record_count = 0
    total_bytes = 0

    while True:
        r = None
        try:
            r, size = read_one_record(fin, raw, verbose, strict, try_snappy)
        except Exception as e:
            if strict:
                fin.close()
                raise e
            elif verbose:
                print e

            if backtrack and type(e) == DecodeError:
                fin.backtrack()
                continue

        if r is None:
            break

        if verbose and r.error is not None:
            print r.error

        record_count += 1
        total_bytes += size

        yield r, total_bytes

    if verbose:
        print "Processed", record_count, "records"

    fin.close()
