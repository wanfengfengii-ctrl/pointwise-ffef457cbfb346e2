# Kafka v2 record-batch fixtures

`writer.py` emits real magic=2 record-batch bytes; `kafka_capture.py` is the
read_committed extraction reader built on top of them.

Run the checks:

```
python3 verify.py          # writer assertions + reader behaviour checks
python3 verify.py demo     # checks, then the visible-record demo as JSON
```

`checks.py` is the original writer fixture assertion script and is left
untouched. `verify.py` re-runs it, then exercises `kafka_capture.Reader`
against the real captured stream and independently constructed wire encodings.
No Kafka service, client, broker or network envelope is used.
