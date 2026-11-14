"""Kafka v2 record-batch writer; no receiving parser or visibility selector."""
import gzip,struct
def crc32c(raw):
    value=0xffffffff
    for octet in raw:
        value^=octet
        for _ in range(8):value=(value>>1)^((0x82f63b78) if value&1 else 0)
    return value^0xffffffff
def vint(value):
    u=(value<<1)^(value>>63);out=bytearray()
    while u>=128:out.append((u&127)|128);u>>=7
    out.append(u);return bytes(out)
def nullable(value):
    return vint(-1) if value is None else vint(len(value))+value
def record(delta,value,key=None,headers=(),timestamp=0):
    body=b'\0'+vint(timestamp)+vint(delta)+nullable(key)+nullable(value)+vint(len(headers))
    for k,v in headers:body+=vint(len(k))+k+nullable(v)
    return vint(len(body))+body
def batch(base,records,producer=-1,epoch=-1,transactional=False,control=False,compressed=False,last=None):
    records=list(records);raw=b''.join(record(**r) for r in records)
    attributes=(1 if compressed else 0)|(16 if transactional else 0)|(32 if control else 0)
    if compressed:raw=gzip.compress(raw,mtime=0)
    if last is None:last=max([r['delta'] for r in records],default=0)
    first_ts=1000;max_ts=max([1000+r.get('timestamp',0) for r in records],default=1000)
    body=struct.pack('>hiqqqhii',attributes,last,first_ts,max_ts,producer,epoch,0,len(records))+raw
    tail=struct.pack('>ibI',0,2,crc32c(body))+body
    return struct.pack('>qi',base,len(tail))+tail
def marker(base,producer,abort=True):
    return batch(base,[{'delta':0,'key':struct.pack('>hh',0,0 if abort else 1),
      'value':struct.pack('>hi',0,0)}],producer=producer,epoch=1,transactional=True,control=True)
def capture():
    return b''.join([
      batch(40,[{'delta':0,'value':b'drop-A'}],7,1,True),
      batch(41,[{'delta':0,'key':None,'value':b'keep-B','headers':[(b'x',None),(b'x',b'2')],'timestamp':-4}],8,1,True,compressed=True),
      marker(42,7,True),
      batch(43,[{'delta':0,'value':None}],7,1,True),
      marker(44,7,False),
      batch(45,[{'delta':0,'value':b'open'}],9,1,True)])
ABORTED=[{'producer_id':7,'first_offset':40}]
LAST_STABLE=45
