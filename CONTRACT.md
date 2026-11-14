# Captured Kafka v2 record batches, read_committed extraction

writer.py generates real Kafka magic=2 record-batch bytes. Input is a single
partition's fetched consecutive batch stream, with immutable fetch metadata.
No Kafka service/client, fetch network envelope, persistence, acknowledgment,
consumer-group rebalance or offset commit is requested.

Implement kafka_capture.Reader(start_offset, last_stable_offset, aborted).
aborted is a list of {producer_id:int64, first_offset:int64} transactions aborted
in this fetch interval; entries are sorted by first_offset, may repeat a producer,
and may begin before start_offset. feed(bytes)->list of record dicts.
finish()->None verifies no incomplete batch remains, then seals the reader.
Record dict is exactly {offset:int, timestamp:int, key:bytes|None, value:bytes|None,
 headers:[(key_bytes,value_bytes|None), ...]}. Preserve header order and duplicates;
None differs from empty bytes. Do not transcode string/header/key/value payloads.
The reader emits user records only, in original offset order, limited to
start_offset <= offset < last_stable_offset (the last stable offset, LSO).
Inputs and constructor types are valid except wire errors expressly listed.

## Physical format
All fixed-width integers are big-endian. Offsets below are relative to batch start.
baseOffset:int64 at0, batchLength:int32 at8 counts bytes AFTER the first12 bytes;
partitionLeaderEpoch:int32 at12, magic:int8 at16=2, crc:uint32 at17.
CRC32C Castagnoli covers every byte from attributes at21 through the batch end.
Header: attributes:int16 at21; lastOffsetDelta:int32 at23; firstTimestamp:int64
at27; maxTimestamp:int64 at35; producerId:int64 at43; producerEpoch:int16 at51;
baseSequence:int32 at53; recordsCount:int32 at57; records start at61.
Header is 61 bytes, batchLength>=49. Compression is attributes&7:
0=uncompressed, 1=gzip. Other codecs are unsupported errors. Bit3 selects log
append timestamp; bit4 transactional; bit5 control. Reject magic!=2,
negative batchLength/lastOffsetDelta/recordsCount, malformed gzip or CRC mismatch.
Ignore other attribute bits and leader/epoch/sequence fields.

After decompression, recordsCount records occupy the entire record area exactly.
Each record starts with zig-zag unsigned-varint encoded signed length, followed
by exactly that many bytes: attributes:int8; timestampDelta:varlong;
offsetDelta:varint; keyLength:varint; key bytes; valueLength:varint; value bytes;
headersCount:varint; then headers of keyLength:varint, key bytes,
valueLength:varint, value bytes. Record attributes are accepted but unused.
key/value/header value length -1 means None, >=0 that many bytes; other negatives
are errors. Header keys cannot be null. headersCount and record length must be
nonnegative. Record bounds must be exact, not merely within batch bounds.
Varint uses LEB128 with zig-zag signed decoding: varint is <=5 octets and signed
32-bit, varlong <=10 octets and signed64; reject overflow and incomplete encodings.
Nonminimal legal encodings are accepted. offsetDelta is nonnegative, strictly
increasing within a batch, <=lastOffsetDelta; gaps and empty compacted batches
are legal. baseOffsets are nonnegative and batches are strictly nonoverlapping:
new baseOffset > previous baseOffset+lastOffsetDelta. Absolute timestamp is
firstTimestamp+timestampDelta unless append-time bit3 is set, then maxTimestamp.
Integer results must remain in signed64 range. Other protocol semantics (for
example whether values fit an application schema) are not validated.

Gzip may contain multiple concatenated members; all members must validate, with
no trailing non-gzip garbage. Decode all before deciding batch validity.
Only a fully verified batch may emit any records: malformed or incomplete batch
never emits a partial list. Previous complete feed results stay valid.

## Transaction visibility
For a transactional non-control batch, skip its records when the matching producer
has an active aborted interval whose first_offset <= batch baseOffset.
An aborted interval starts at that producer's first_offset, ends at its next ABORT
control marker batch, and must not hide a later transaction by the same producer.
Activate pending aborted entries before each batch at its baseOffset, even if the
fetch starts in an already-aborted transaction. Input metadata/transaction ordering
is valid: no overlapping transaction intervals for one producer; a later aborted
interval for that producer starts after the previous marker.
Control batches are transactional, uncompressed, with exactly one record at
delta0. Its key is big-endian int16 version0 + int16 type (0 ABORT,1 COMMIT);
value is int16 version0 + int32 coordinatorEpoch. Reject malformed control key,
value or flags. Control batches never emit user records. ABORT ends that producer's
active aborted interval; COMMIT does not create one. A marker for a transaction
not present in aborted metadata is legal. producer epoch is not a partition offset
and must not be used to match the metadata. Nontransactional batches are not hidden
by an aborted interval. Aborted markers may occur after the LSO/request range;
they still update extraction state. All batches, including filtered/out-of-range
or aborted ones, undergo complete framing, checksum and record validation.
Do not treat every batch for an aborted producer as permanently invisible.

finish requires complete physical input but does not require all pending aborted
intervals to end inside the fetch. Repeated successful finish returns None;
feed after finish, or any malformed/unsupported input, raises kafka_capture.DecodeError
and permanently fails the reader. Subsequent feed/finish raise DecodeError.
Between calls, retain only a partial physical batch plus metadata for visibility.
While processing a complete batch, compressed/decompressed current-batch data and
record objects are permitted. Do not keep the full fetched partition or past
record payloads. Returned output/caller input is excluded; no arbitrary extra
resource threshold is introduced.

capture() with start_offset=40, LAST_STABLE and ABORTED emits offsets41 and43:
41 is keep-B, timestamp996, null key and [(b'x',None),(b'x',b'2')];
43 is a tombstone (value None), timestamp1000. Offset40 is aborted; after marker42
producer7's later transaction is visible; markers emit nothing and offset45 is
outside the stable prefix.
Keep writer.py and checks.py; add the extractor, business validation and real demo.
Use standard library; update the existing runtime manifest to run target behavior.
