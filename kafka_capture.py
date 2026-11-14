"""Read_committed extraction from captured Kafka magic=2 record batches.

The reader accepts a fetched stream in arbitrarily sliced ``bytes`` input and
emits only persistent user records that are visible under the supplied start
offset, last stable offset (LSO) and aborted-transaction metadata.

Standard library only.  No Kafka client, broker or network envelope is needed.
"""

from __future__ import annotations

import struct
import zlib

__all__ = ["DecodeError", "Reader"]

_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1

_BATCH_HEADER_SIZE = 61
_BATCH_PREFIX_SIZE = 12  # baseOffset + batchLength
_MIN_BATCH_LENGTH = _BATCH_HEADER_SIZE - _BATCH_PREFIX_SIZE


class DecodeError(Exception):
    """A malformed, unsupported or truncated captured batch stream."""


def _make_crc32c_table():
    polynomial = 0x82F63B78
    table = []
    for index in range(256):
        value = index
        for _ in range(8):
            value = (value >> 1) ^ (polynomial if value & 1 else 0)
        table.append(value)
    return tuple(table)


_CRC32C_TABLE = _make_crc32c_table()


def crc32c(data: bytes) -> int:
    """CRC32C (Castagnoli) as used by Kafka record batch headers."""
    value = 0xFFFFFFFF
    table = _CRC32C_TABLE
    for octet in data:
        value = (value >> 8) ^ table[(value ^ octet) & 0xFF]
    return value ^ 0xFFFFFFFF


def _read_zigzag_varint(data: bytes, position: int, bits: int):
    """Decode one LEB128 zig-zag integer and return ``(value, new_position)``.

    ``bits`` is 32 (at most five octets) or 64 (at most ten octets).  Both
    incomplete values and values with set bits beyond the target width are
    rejected.  Non-minimal legal encodings are deliberately accepted.
    """
    max_octets = 5 if bits == 32 else 10
    unsigned = 0
    for index in range(max_octets):
        if position >= len(data):
            raise DecodeError("incomplete zig-zag varint")
        octet = data[position]
        position += 1
        if index == max_octets - 1:
            # Only the bits that still fit in the target width may be set in
            # the final octet, and its continuation bit must be clear.
            if bits == 32:
                if octet & 0xF0:
                    raise DecodeError("varint overflows signed 32-bit range")
            else:
                if octet & 0xFE:
                    raise DecodeError("varlong overflows signed 64-bit range")
        unsigned |= (octet & 0x7F) << (7 * index)
        if not (octet & 0x80):
            value = (unsigned >> 1) ^ -(unsigned & 1)
            return value, position
    # The final-octet check above always raises for a still-continuing value.
    raise DecodeError("varint overflow")


def _decode_gzip_members(data: bytes) -> bytes:
    """Decompress one or more concatenated gzip members, rejecting garbage."""
    if not data:
        raise DecodeError("malformed gzip: empty compressed record area")

    output = bytearray()
    position = 0
    total = len(data)
    while position < total:
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            chunk = decompressor.decompress(data[position:])
        except zlib.error as exc:
            raise DecodeError("malformed gzip: %s" % (exc,)) from None
        if not decompressor.eof:
            raise DecodeError("malformed gzip: truncated member")
        output += chunk
        consumed = (total - position) - len(decompressor.unused_data)
        if consumed <= 0:
            raise DecodeError("malformed gzip: no progress")
        position += consumed
    return bytes(output)


def _read_bytes_field(body: bytes, position: int):
    """Read a Kafka nullable-bytes length prefix and its payload."""
    length, position = _read_zigzag_varint(body, position, 32)
    if length == -1:
        return None, position
    if length < 0:
        raise DecodeError("invalid negative bytes length")
    end = position + length
    if end > len(body):
        raise DecodeError("bytes field exceeds record boundary")
    return body[position:end], end


def _decode_record_body(body: bytes):
    """Decode one record body (the bytes after the record length prefix)."""
    if not body:
        raise DecodeError("record body is empty")

    # attributes:int8 is accepted but unused.
    position = 1
    timestamp_delta, position = _read_zigzag_varint(body, position, 64)
    offset_delta, position = _read_zigzag_varint(body, position, 32)
    key, position = _read_bytes_field(body, position)
    value, position = _read_bytes_field(body, position)
    headers_count, position = _read_zigzag_varint(body, position, 32)
    if headers_count < 0:
        raise DecodeError("negative headers count")

    headers = []
    for _ in range(headers_count):
        header_key_length, position = _read_zigzag_varint(body, position, 32)
        if header_key_length < 0:
            raise DecodeError("header key cannot be null or negative-length")
        key_end = position + header_key_length
        if key_end > len(body):
            raise DecodeError("header key exceeds record boundary")
        header_key = body[position:key_end]
        position = key_end

        header_value_length, position = _read_zigzag_varint(body, position, 32)
        if header_value_length == -1:
            header_value = None
        elif header_value_length < 0:
            raise DecodeError("invalid negative header value length")
        else:
            value_end = position + header_value_length
            if value_end > len(body):
                raise DecodeError("header value exceeds record boundary")
            header_value = body[position:value_end]
            position = value_end
        headers.append((header_key, header_value))

    if position != len(body):
        raise DecodeError("record length does not match record contents")
    return timestamp_delta, offset_delta, key, value, headers


def _decode_record_area(
    area: bytes,
    records_count: int,
    base_offset: int,
    last_offset_delta: int,
    first_timestamp: int,
    max_timestamp: int,
    log_append_time: bool,
):
    """Decode every record, validating exact boundaries and offset ordering."""
    records = []
    position = 0
    previous_offset_delta = -1
    for _ in range(records_count):
        record_length, position = _read_zigzag_varint(area, position, 32)
        if record_length < 0:
            raise DecodeError("negative record length")
        record_end = position + record_length
        if record_end > len(area):
            raise DecodeError("record exceeds record area boundary")
        body = area[position:record_end]
        position = record_end

        timestamp_delta, offset_delta, key, value, headers = _decode_record_body(body)
        if offset_delta < 0:
            raise DecodeError("negative record offset delta")
        if offset_delta <= previous_offset_delta:
            raise DecodeError("record offset deltas are not strictly increasing")
        if offset_delta > last_offset_delta:
            raise DecodeError("record offset delta exceeds lastOffsetDelta")
        previous_offset_delta = offset_delta

        if log_append_time:
            timestamp = max_timestamp
        else:
            timestamp = first_timestamp + timestamp_delta
        if timestamp < _I64_MIN or timestamp > _I64_MAX:
            raise DecodeError("record timestamp overflows signed 64-bit range")

        offset = base_offset + offset_delta
        if offset < _I64_MIN or offset > _I64_MAX:
            raise DecodeError("record offset overflows signed 64-bit range")

        records.append((offset, timestamp, key, value, headers))

    if position != len(area):
        raise DecodeError("records do not occupy the record area exactly")
    return records


class Reader:
    """Incremental read_committed extractor for one partition's batch stream."""

    def __init__(self, start_offset: int, last_stable_offset: int, aborted):
        self._start_offset = start_offset
        self._last_stable_offset = last_stable_offset

        # Pending aborted transactions, kept in the documented first_offset
        # order; entries may predate start_offset and may repeat a producer.
        # A stable sort keeps the contract order when metadata is already
        # sorted and makes equal first_offsets deterministic.
        self._pending_aborted = sorted(
            (
                (int(entry["first_offset"]), int(entry["producer_id"]))
                for entry in aborted
            ),
            key=lambda item: item[0],
        )
        self._pending_index = 0

        # producer_id -> first_offset of its currently active aborted interval.
        self._active_aborted = {}

        # Physical framing and batch-ordering metadata that must survive calls.
        self._buffer = bytearray()
        self._previous_batch_end = None

        self._finished = False
        self._failed = False

    # -- public API ---------------------------------------------------------

    def feed(self, data) -> list:
        if self._failed:
            raise DecodeError("reader has permanently failed")
        if self._finished:
            self._failed = True
            raise DecodeError("feed() called after finish()")

        self._buffer += bytes(data)
        output = []
        try:
            while True:
                if len(self._buffer) < _BATCH_PREFIX_SIZE:
                    break

                base_offset, batch_length = struct.unpack_from(
                    ">qi", self._buffer, 0
                )
                if batch_length < _MIN_BATCH_LENGTH:
                    raise DecodeError("negative or too-small batchLength")
                total_length = _BATCH_PREFIX_SIZE + batch_length
                if len(self._buffer) < total_length:
                    break

                batch = bytes(self._buffer[:total_length])
                del self._buffer[:total_length]
                output.extend(self._process_batch(batch, base_offset, batch_length))
        except DecodeError:
            self._failed = True
            raise
        except Exception as exc:  # defensive: any decoder bug is a hard failure
            self._failed = True
            raise DecodeError("failed to decode batch: %s" % (exc,)) from exc
        return output

    def finish(self):
        if self._failed:
            raise DecodeError("reader has permanently failed")
        if self._buffer:
            self._failed = True
            raise DecodeError("truncated batch remains at end of input")
        self._finished = True
        return None

    # -- batch processing ---------------------------------------------------

    def _activate_aborted(self, base_offset: int) -> None:
        pending = self._pending_aborted
        while self._pending_index < len(pending):
            first_offset, producer_id = pending[self._pending_index]
            if first_offset > base_offset:
                break
            self._active_aborted[producer_id] = first_offset
            self._pending_index += 1

    def _process_batch(self, batch: bytes, base_offset: int, batch_length: int):
        total_length = _BATCH_PREFIX_SIZE + batch_length

        if batch[16] != 2:
            raise DecodeError("unsupported record batch magic")
        if base_offset < 0:
            raise DecodeError("negative batch baseOffset")

        stored_crc = struct.unpack_from(">I", batch, 17)[0]
        calculated_crc = crc32c(batch[21:total_length])
        if stored_crc != calculated_crc:
            raise DecodeError("record batch CRC32C mismatch")

        attributes = struct.unpack_from(">h", batch, 21)[0]
        last_offset_delta = struct.unpack_from(">i", batch, 23)[0]
        first_timestamp = struct.unpack_from(">q", batch, 27)[0]
        max_timestamp = struct.unpack_from(">q", batch, 35)[0]
        producer_id = struct.unpack_from(">q", batch, 43)[0]
        records_count = struct.unpack_from(">i", batch, 57)[0]

        if last_offset_delta < 0:
            raise DecodeError("negative lastOffsetDelta")
        if records_count < 0:
            raise DecodeError("negative recordsCount")

        batch_end = base_offset + last_offset_delta
        if batch_end < _I64_MIN or batch_end > _I64_MAX:
            raise DecodeError("batch offset range overflows signed 64-bit")
        if (
            self._previous_batch_end is not None
            and base_offset <= self._previous_batch_end
        ):
            raise DecodeError("overlapping or non-increasing record batches")

        codec = attributes & 0x07
        if codec == 0:
            record_area = batch[_BATCH_HEADER_SIZE:total_length]
        elif codec == 1:
            record_area = _decode_gzip_members(
                batch[_BATCH_HEADER_SIZE:total_length]
            )
        else:
            raise DecodeError("unsupported compression codec")

        log_append_time = bool(attributes & 0x08)
        transactional = bool(attributes & 0x10)
        control = bool(attributes & 0x20)

        # A control batch has stricter flags than an ordinary batch.
        if control and (not transactional or codec != 0):
            raise DecodeError("malformed control batch flags")

        # Every batch, even one that will be filtered, is fully decoded and
        # validated before any record is emitted.
        records = _decode_record_area(
            record_area,
            records_count,
            base_offset,
            last_offset_delta,
            first_timestamp,
            max_timestamp,
            log_append_time,
        )

        # Aborted-interval activation happens at each batch's baseOffset, so a
        # fetch starting inside an already-aborted transaction behaves the same
        # as one starting at its first batch.
        self._activate_aborted(base_offset)
        self._previous_batch_end = batch_end

        if control:
            self._validate_control_batch(records, producer_id, base_offset)
            return []

        if transactional and producer_id in self._active_aborted:
            return []

        output = []
        start = self._start_offset
        stable = self._last_stable_offset
        for offset, timestamp, key, value, headers in records:
            if start <= offset < stable:
                output.append(
                    {
                        "offset": offset,
                        "timestamp": timestamp,
                        "key": key,
                        "value": value,
                        "headers": headers,
                    }
                )
        return output

    def _validate_control_batch(self, records, producer_id: int, base_offset: int) -> None:
        if len(records) != 1:
            raise DecodeError("control batch must contain exactly one record")
        offset, _timestamp, key, value, _headers = records[0]
        if offset != base_offset:
            raise DecodeError("control record must use offset delta 0")

        if key is None or len(key) != 4:
            raise DecodeError("malformed control key")
        version, marker_type = struct.unpack(">hh", key)
        if version != 0 or marker_type not in (0, 1):
            raise DecodeError("malformed control key")

        if value is None or len(value) != 6:
            raise DecodeError("malformed control value")
        value_version, _coordinator_epoch = struct.unpack(">hi", value)
        if value_version != 0:
            raise DecodeError("malformed control value")

        if marker_type == 0:  # ABORT
            self._active_aborted.pop(producer_id, None)
        # COMMIT neither creates nor ends an aborted interval.
