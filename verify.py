"""Business validation and runnable demo for :mod:`kafka_capture`.

The check suite first re-runs the untouched ``checks.py`` byte-writer fixture
assertions, then round-trips the real captured stream through ``Reader`` under
many slicings and adversarial encodings.  Run with ``demo`` to print the
visible records from the captured sample, including the transaction that
becomes visible after producer 7's ABORT marker.
"""

import gzip
import json
import os
import struct
import subprocess
import sys

from kafka_capture import DecodeError, Reader
from writer import ABORTED, LAST_STABLE, capture, crc32c, vint

HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Fixture builders (independent of the writer's convenience function, so that
# malformed encodings can be constructed deliberately).
# ---------------------------------------------------------------------------


def _nullable(value):
    return vint(-1) if value is None else vint(len(value)) + value


def _record_body(
    timestamp_delta=0, offset_delta=0, key=None, value=b"", headers=(), attributes=0
):
    body = bytes([attributes & 0xFF]) + vint(timestamp_delta) + vint(offset_delta)
    body += _nullable(key) + _nullable(value) + vint(len(headers))
    for header_key, header_value in headers:
        body += vint(len(header_key)) + header_key + _nullable(header_value)
    return body


def _record(body):
    return vint(len(body)) + body


def _batch(
    base,
    bodies=(),
    *,
    attributes=0,
    last_offset_delta=0,
    first_timestamp=1000,
    max_timestamp=None,
    producer_id=-1,
    producer_epoch=-1,
    base_sequence=0,
    records_count=None,
    record_area=None,
    magic=2,
    crc=None,
    batch_length=None,
    leader_epoch=0,
):
    if record_area is None:
        record_area = b"".join(_record(body) for body in bodies)
    if records_count is None:
        records_count = len(bodies)
    if max_timestamp is None:
        max_timestamp = first_timestamp
    body = struct.pack(
        ">hiqqqhii",
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records_count,
    ) + record_area
    if crc is None:
        crc = crc32c(body)
    tail = struct.pack(">ibI", leader_epoch, magic, crc) + body
    length = len(tail) if batch_length is None else batch_length
    return struct.pack(">qi", base, length) + tail


def _marker(base, producer_id, abort=True, *, key=None, value=None, attributes=None):
    if key is None:
        key = struct.pack(">hh", 0, 0 if abort else 1)
    if value is None:
        value = struct.pack(">hi", 0, 0)
    body = _record_body(key=key, value=value)
    if attributes is None:
        attributes = 0x10 | 0x20
    return _batch(
        base,
        [body],
        attributes=attributes,
        producer_id=producer_id,
        producer_epoch=1,
    )


def _expect_decode_error(reader, stream, label):
    try:
        reader.feed(stream)
    except DecodeError:
        pass
    except Exception as exc:  # noqa: BLE001 - any other error is a test failure
        raise AssertionError("%s raised %r instead of DecodeError" % (label, exc))
    else:
        raise AssertionError("%s did not raise DecodeError" % label)
    # A wire error permanently fails the reader.
    for name, action in (("feed", lambda: reader.feed(b"")), ("finish", reader.finish)):
        try:
            action()
        except DecodeError:
            pass
        else:
            raise AssertionError("%s stayed usable after failure" % label)
    return reader


def _feed_in_chunks(raw, size):
    reader = Reader(40, LAST_STABLE, ABORTED)
    output = []
    for position in range(0, len(raw), size):
        output.extend(reader.feed(raw[position : position + size]))
    reader.finish()
    return output


def _boundaries(raw):
    positions = [0]
    offset = 0
    while offset < len(raw):
        _base, length = struct.unpack_from(">qi", raw, offset)
        offset += 12 + length
        positions.append(offset)
    return positions


# ---------------------------------------------------------------------------
# Original fixture checks.
# ---------------------------------------------------------------------------


def check_original_fixture():
    result = subprocess.run(
        [sys.executable, os.path.join(HERE, "checks.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    sys.stdout.write(result.stdout.decode("utf-8", "replace"))


# ---------------------------------------------------------------------------
# Reader behaviour checks.
# ---------------------------------------------------------------------------


def check_crc32c():
    assert crc32c(b"123456789") == 0xE3069283
    assert crc32c(b"") == 0


def check_capture_visibility():
    raw = capture()
    reader = Reader(40, LAST_STABLE, ABORTED)
    output = reader.feed(raw)
    assert output == [
        {
            "offset": 41,
            "timestamp": 996,
            "key": None,
            "value": b"keep-B",
            "headers": [(b"x", None), (b"x", b"2")],
        },
        {
            "offset": 43,
            "timestamp": 1000,
            "key": None,
            "value": None,
            "headers": [],
        },
    ], output
    assert reader.finish() is None
    assert reader.finish() is None  # repeated successful finish

    # The later transaction from producer 7 (offset 43) is visible after the
    # ABORT marker at offset 42; offset 45 is beyond the stable prefix.
    assert all(record["offset"] != 40 for record in output)
    assert all(record["offset"] != 45 for record in output)
    assert [record["offset"] for record in output] == [41, 43]


def check_slicing_invariance():
    raw = capture()
    expected = _feed_in_chunks(raw, len(raw))
    for size in range(1, 20):
        assert _feed_in_chunks(raw, size) == expected, size
    # One-byte slicing exercises both the batch-length and record boundaries.
    assert _feed_in_chunks(raw, 1) == expected


def check_finish_after_finish():
    reader = Reader(40, LAST_STABLE, ABORTED)
    assert reader.feed(capture())
    assert reader.finish() is None
    try:
        reader.feed(b"")
    except DecodeError:
        pass
    else:
        raise AssertionError("feed after finish was accepted")
    try:
        reader.finish()
    except DecodeError:
        pass
    else:
        raise AssertionError("finish after failed feed was accepted")


def check_truncation():
    raw = capture()
    boundaries = set(_boundaries(raw))
    assert max(boundaries) == len(raw)
    for cut in range(len(raw) + 1):
        reader = Reader(40, LAST_STABLE, ABORTED)
        reader.feed(raw[:cut])
        if cut in boundaries:
            assert reader.finish() is None
        else:
            try:
                reader.finish()
            except DecodeError:
                pass
            else:
                raise AssertionError("truncated stream at %d was accepted" % cut)
            try:
                reader.feed(b"")
            except DecodeError:
                pass
            else:
                raise AssertionError("truncated reader was not failed permanently")


def check_offset_window():
    raw = capture()
    # start_offset is inclusive, LSO is exclusive.
    reader = Reader(41, 43, ABORTED)
    assert [r["offset"] for r in reader.feed(raw)] == [41]
    reader = Reader(0, 46, [])
    assert [r["offset"] for r in reader.feed(raw)] == [40, 41, 43, 45]
    reader = Reader(0, 45, [])
    assert [r["offset"] for r in reader.feed(raw)] == [40, 41, 43]
    # An empty window emits nothing but still validates the whole stream.
    reader = Reader(100, 200, ABORTED)
    assert reader.feed(raw) == []
    reader.finish()


def check_timestamp_modes():
    normal = _batch(
        10,
        [_record_body(timestamp_delta=7), _record_body(offset_delta=1, timestamp_delta=-3)],
        last_offset_delta=1,
        first_timestamp=1000,
        max_timestamp=2000,
    )
    log_append = _batch(
        20,
        [_record_body(timestamp_delta=7), _record_body(offset_delta=1, timestamp_delta=-3)],
        attributes=0x08,
        last_offset_delta=1,
        first_timestamp=1000,
        max_timestamp=2000,
    )
    reader = Reader(0, 100, [])
    output = reader.feed(normal + log_append)
    assert [r["timestamp"] for r in output] == [1007, 997, 2000, 2000]


def check_compaction_gaps_and_empty_batches():
    gaps = _batch(
        100,
        [
            _record_body(offset_delta=0, value=b"a"),
            _record_body(offset_delta=3, value=b"b"),
            _record_body(offset_delta=9, value=b"c"),
        ],
        last_offset_delta=9,
    )
    empty = _batch(200, [], last_offset_delta=0, records_count=0)
    after = _batch(201, [_record_body(value=b"d")], last_offset_delta=0)
    reader = Reader(0, 1000, [])
    output = reader.feed(gaps + empty + after)
    assert [r["offset"] for r in output] == [100, 103, 109, 201]
    assert [r["value"] for r in output] == [b"a", b"b", b"c", b"d"]
    reader.finish()


def check_null_versus_empty():
    body = _record_body(
        key=None,
        value=None,
        headers=[(b"h", None), (b"h", b""), (b"", b"")],
    )
    second = _record_body(offset_delta=1, key=b"", value=b"")
    stream = _batch(10, [body, second], last_offset_delta=1)
    reader = Reader(0, 100, [])
    output = reader.feed(stream)
    assert output[0]["key"] is None and output[0]["value"] is None
    assert output[0]["headers"] == [(b"h", None), (b"h", b""), (b"", b"")]
    assert output[1]["key"] == b"" and output[1]["value"] == b""
    assert output[1]["headers"] == []


def check_nonminimal_varints():
    # offsetDelta 0 and value length 0 encoded with redundant continuation.
    body = b"\x00" + vint(0) + b"\x80\x00" + _nullable(None) + b"\x80\x00" + vint(0)
    stream = _batch(10, [body], last_offset_delta=0)
    reader = Reader(0, 100, [])
    output = reader.feed(stream)
    assert output[0]["offset"] == 10 and output[0]["value"] == b""


def check_gzip_members():
    first = _record(_record_body(value=b"a"))
    second = _record(_record_body(offset_delta=1, value=b"b"))
    members = gzip.compress(first, mtime=0) + gzip.compress(second, mtime=0)
    stream = _batch(
        50,
        attributes=0x01,
        last_offset_delta=1,
        records_count=2,
        record_area=members,
    )
    reader = Reader(0, 100, [])
    output = reader.feed(stream)
    assert [r["offset"] for r in output] == [50, 51]
    assert [r["value"] for r in output] == [b"a", b"b"]

    # A corrupt or truncated later member invalidates the whole batch.
    bad_second = gzip.compress(first, mtime=0) + gzip.compress(second, mtime=0)[:-2]
    _expect_decode_error(
        Reader(0, 100, []),
        _batch(
            50,
            attributes=0x01,
            last_offset_delta=1,
            records_count=2,
            record_area=bad_second,
        ),
        "truncated trailing gzip member",
    )
    # Trailing non-gzip garbage is not ignored.
    _expect_decode_error(
        Reader(0, 100, []),
        _batch(
            50,
            attributes=0x01,
            last_offset_delta=1,
            records_count=2,
            record_area=members + b"garbage",
        ),
        "trailing gzip garbage",
    )


def check_aborted_transactions():
    # A later transaction by the same producer is visible after ABORT.
    stream = b"".join(
        [
            _batch(10, [_record_body(value=b"aborted")], producer_id=7, attributes=0x10),
            _marker(11, 7, abort=True),
            _batch(12, [_record_body(value=b"later")], producer_id=7, attributes=0x10),
        ]
    )
    metadata = [{"producer_id": 7, "first_offset": 10}]
    reader = Reader(0, 100, metadata)
    output = reader.feed(stream)
    assert [(r["offset"], r["value"]) for r in output] == [(12, b"later")], output

    # COMMIT does not end an aborted interval; only ABORT does.
    stream = b"".join(
        [
            _batch(10, [_record_body(value=b"aborted")], producer_id=7, attributes=0x10),
            _marker(11, 7, abort=False),
            _batch(12, [_record_body(value=b"still-hidden")], producer_id=7, attributes=0x10),
        ]
    )
    reader = Reader(0, 100, metadata)
    assert reader.feed(stream) == []

    # Nontransactional batches are never hidden by aborted metadata.
    stream = _batch(10, [_record_body(value=b"plain")], producer_id=7, attributes=0)
    reader = Reader(0, 100, metadata)
    assert [r["value"] for r in reader.feed(stream)] == [b"plain"]

    # A marker for an unknown producer is legal.
    reader = Reader(0, 100, [])
    assert reader.feed(_marker(5, 99, abort=True)) == []
    reader.finish()

    # The aborted interval was activated even though the fetch begins inside
    # it, and a repeat of the same producer is still tracked independently.
    stream = b"".join(
        [
            _batch(10, [_record_body(value=b"a")], producer_id=7, attributes=0x10),
            _batch(11, [_record_body(value=b"b")], producer_id=7, attributes=0x10),
            _marker(12, 7, abort=True),
            _batch(13, [_record_body(value=b"c")], producer_id=7, attributes=0x10),
            _marker(14, 7, abort=True),
            _batch(15, [_record_body(value=b"d")], producer_id=7, attributes=0x10),
        ]
    )
    metadata = [
        {"producer_id": 7, "first_offset": 10},
        {"producer_id": 7, "first_offset": 13},
    ]
    reader = Reader(0, 100, metadata)
    assert [r["value"] for r in reader.feed(stream)] == [b"d"]

    # A producer epoch is not used to match the metadata.
    stream = _batch(
        10,
        [_record_body(value=b"hidden")],
        producer_id=7,
        producer_epoch=999,
        attributes=0x10,
    )
    assert Reader(0, 100, metadata[:1]).feed(stream) == []

    # A fetch may end with an aborted interval still open.
    reader = Reader(0, 100, [{"producer_id": 7, "first_offset": 10}])
    assert reader.feed(
        _batch(10, [_record_body(value=b"hidden")], producer_id=7, attributes=0x10)
    ) == []
    reader.finish()

    # Markers beyond the LSO are still validated and still update state.
    stream = b"".join(
        [
            _batch(10, [_record_body(value=b"hidden")], producer_id=7, attributes=0x10),
            _marker(11, 7, abort=True),
            _batch(12, [_record_body(value=b"visible")], producer_id=7, attributes=0x10),
        ]
    )
    reader = Reader(0, 11, [{"producer_id": 7, "first_offset": 10}])
    assert reader.feed(stream) == []

    # A malformed marker after the LSO is still an error.
    bad_marker = _marker(11, 7, abort=True, key=b"\x00\x00\x00")
    _expect_decode_error(
        Reader(0, 11, []),
        b"".join(
            [
                _batch(10, [_record_body(value=b"x")], producer_id=7, attributes=0x10),
                bad_marker,
            ]
        ),
        "malformed control marker beyond LSO",
    )


def check_wire_errors():
    record = _record_body(value=b"x")
    cases = {
        "bad magic": _batch(10, [record], magic=1),
        "bad crc": _batch(10, [record], crc=0),
        "negative batch length": struct.pack(">qi", 0, -1),
        "batch length too small": struct.pack(">qi", 0, 48)
        + struct.pack(">ibI", 0, 2, 0)
        + b"\x00" * 48,
        "negative last offset delta": _batch(10, [record], last_offset_delta=-1),
        "negative records count": _batch(10, [], records_count=-1),
        "negative base offset": _batch(-1, [record], last_offset_delta=0),
        "unsupported codec": _batch(10, [record], attributes=2),
        "record count mismatch": _batch(10, [record], records_count=2),
        "record area trailing byte": _batch(
            10, [], last_offset_delta=0, records_count=1, record_area=record + b"\x00"
        ),
        "negative record length": _batch(
            10, [], last_offset_delta=0, records_count=1, record_area=vint(-1)
        ),
        "zero record length": _batch(
            10, [], last_offset_delta=0, records_count=1, record_area=vint(0)
        ),
        "key length -2": _batch(
            10,
            [b"\x00" + vint(0) + vint(0) + vint(-2) + vint(0) + vint(0)],
            last_offset_delta=0,
        ),
        "null header key": _batch(
            10,
            [
                b"\x00"
                + vint(0)
                + vint(0)
                + _nullable(None)
                + _nullable(b"v")
                + vint(1)
                + vint(-1)
                + vint(0)
            ],
            last_offset_delta=0,
        ),
        "header value length -2": _batch(
            10,
            [
                b"\x00"
                + vint(0)
                + vint(0)
                + _nullable(None)
                + _nullable(b"v")
                + vint(1)
                + vint(0)
                + vint(-2)
            ],
            last_offset_delta=0,
        ),
        "negative headers count": _batch(
            10,
            [
                b"\x00"
                + vint(0)
                + vint(0)
                + _nullable(None)
                + _nullable(b"v")
                + vint(-1)
            ],
            last_offset_delta=0,
        ),
        "varint 32-bit overflow": _batch(
            10,
            [
                b"\x00"
                + vint(0)
                + b"\x80\x80\x80\x80\x10"
                + _nullable(None)
                + _nullable(b"v")
                + vint(0)
            ],
            last_offset_delta=0,
        ),
        "varlong 64-bit overflow": _batch(
            10,
            [
                b"\x00"
                + b"\x80" * 9
                + b"\x02"
                + vint(0)
                + _nullable(None)
                + _nullable(b"v")
                + vint(0)
            ],
            last_offset_delta=0,
        ),
        "incomplete varint": _batch(
            10,
            [b"\x00\x80"],
            last_offset_delta=0,
        ),
        "negative offset delta": _batch(
            10, [_record_body(offset_delta=-1)], last_offset_delta=0
        ),
        "duplicate offset delta": _batch(
            10,
            [_record_body(offset_delta=1), _record_body(offset_delta=1)],
            last_offset_delta=1,
        ),
        "offset delta beyond last": _batch(
            10, [_record_body(offset_delta=1)], last_offset_delta=0
        ),
        "timestamp overflow": _batch(
            10,
            [_record_body(timestamp_delta=1)],
            last_offset_delta=0,
            first_timestamp=(1 << 63) - 1,
            max_timestamp=(1 << 63) - 1,
        ),
        "non-transactional control": _marker(10, 7, abort=True, attributes=0x20),
        "control with short key": _marker(10, 7, abort=True, key=b"\x00\x00\x00"),
        "control with bad key version": _marker(
            10, 7, abort=True, key=struct.pack(">hh", 1, 0)
        ),
        "control with bad marker type": _marker(
            10, 7, abort=True, key=struct.pack(">hh", 0, 2)
        ),
        "control with null key": _batch(
            10,
            [_record_body(key=None, value=struct.pack(">hi", 0, 0))],
            attributes=0x30,
            producer_id=7,
            producer_epoch=1,
        ),
        "control with short value": _marker(10, 7, abort=True, value=b"\x00"),
        "control with bad value version": _marker(
            10, 7, abort=True, value=struct.pack(">hi", 1, 0)
        ),
        "control with two records": _batch(
            10,
            [
                _record_body(key=struct.pack(">hh", 0, 0), value=struct.pack(">hi", 0, 0)),
                _record_body(
                    offset_delta=1,
                    key=struct.pack(">hh", 0, 0),
                    value=struct.pack(">hi", 0, 0),
                ),
            ],
            attributes=0x30,
            last_offset_delta=1,
            producer_id=7,
            producer_epoch=1,
        ),
        "control record not at delta zero": _batch(
            10,
            [
                _record_body(
                    offset_delta=1,
                    key=struct.pack(">hh", 0, 0),
                    value=struct.pack(">hi", 0, 0),
                )
            ],
            attributes=0x30,
            last_offset_delta=1,
            producer_id=7,
            producer_epoch=1,
        ),
    }
    for label, stream in cases.items():
        _expect_decode_error(Reader(0, 100, []), stream, label)

    # Overlapping or repeated batch base offsets.
    first = _batch(10, [_record_body(value=b"a")], last_offset_delta=0)
    overlapping = _batch(10, [_record_body(value=b"b")], last_offset_delta=0)
    _expect_decode_error(Reader(0, 100, []), first + overlapping, "overlapping batches")


def check_filtered_batches_still_validate():
    # The aborted batch's payload is malformed even though it would be hidden.
    bad_records = _record(_record_body(value=b"hidden")) + b"\x00"
    stream = b"".join(
        [
            _batch(
                10,                [],
                attributes=0x10,
                producer_id=7,
                last_offset_delta=0,
                records_count=1,
                record_area=bad_records,
            ),
            _batch(11, [_record_body(value=b"visible")], producer_id=7, attributes=0x10),
        ]
    )
    _expect_decode_error(
        Reader(0, 100, [{"producer_id": 7, "first_offset": 10}]),
        stream,
        "malformed aborted batch",
    )

    # The same is true beyond the LSO: offset 50 is out of range but malformed.
    bad_out_of_range = _batch(50, [b"\x00"], last_offset_delta=0)
    _expect_decode_error(
        Reader(0, 10, []), bad_out_of_range, "malformed out-of-range batch"
    )


def check_batch_ordering_gap():
    # Ordinary gaps between batches are legal; only overlap is rejected.
    stream = b"".join(
        [
            _batch(10, [_record_body(value=b"a")], last_offset_delta=2),
            _batch(20, [_record_body(value=b"b")], last_offset_delta=0),
        ]
    )
    reader = Reader(0, 100, [])
    assert [r["offset"] for r in reader.feed(stream)] == [10, 20]


def check_failure_is_sticky():
    reader = Reader(0, 100, [])
    try:
        reader.feed(_batch(10, [_record_body(value=b"x")], crc=1))
    except DecodeError:
        pass
    else:
        raise AssertionError("CRC mismatch was accepted")
    for name, action in (("feed", lambda: reader.feed(b"")), ("finish", reader.finish)):
        try:
            action()
        except DecodeError:
            pass
        else:
            raise AssertionError("reader recovered after a wire error (%s)" % name)


# ---------------------------------------------------------------------------
# Demo.
# ---------------------------------------------------------------------------


def _display(value):
    if value is None:
        return None
    return {"utf8": value.decode("utf-8", "backslashreplace"), "hex": value.hex()}


def demo():
    raw = capture()
    reader = Reader(40, LAST_STABLE, ABORTED)
    visible = reader.feed(raw)
    reader.finish()
    rendered = [
        {
            "offset": record["offset"],
            "timestamp": record["timestamp"],
            "key": _display(record["key"]),
            "value": _display(record["value"]),
            "headers": [[_display(k), _display(v)] for k, v in record["headers"]],
        }
        for record in visible
    ]
    print(
        json.dumps(
            {
                "stream_bytes": len(raw),
                "start_offset": 40,
                "last_stable_offset": LAST_STABLE,
                "aborted": ABORTED,
                "visible_records": rendered,
                "notes": {
                    "offset_40": "producer 7 transaction aborted: not emitted",
                    "offset_41": "producer 8 transaction committed and visible",
                    "offset_42": "ABORT control marker: emits nothing",
                    "offset_43": "producer 7's next transaction, visible after the ABORT marker",
                    "offset_44": "COMMIT control marker: emits nothing",
                    "offset_45": "at/after the last stable offset: not emitted",
                },
                "later_transaction_after_abort": 43,
            },
            indent=2,
            sort_keys=False,
        )
    )


def main():
    check_original_fixture()
    check_crc32c()
    check_capture_visibility()
    check_slicing_invariance()
    check_finish_after_finish()
    check_truncation()
    check_offset_window()
    check_timestamp_modes()
    check_compaction_gaps_and_empty_batches()
    check_null_versus_empty()
    check_nonminimal_varints()
    check_gzip_members()
    check_aborted_transactions()
    check_wire_errors()
    check_filtered_batches_still_validate()
    check_batch_ordering_gap()
    check_failure_is_sticky()
    print("KAFKA_CAPTURE_READER_OK")
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        demo()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
