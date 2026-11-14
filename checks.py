import sys,json,struct,hashlib
from writer import crc32c,vint,capture,ABORTED,LAST_STABLE
assert crc32c(b'123456789')==0xe3069283
assert [vint(x).hex() for x in [0,-1,1,64,-64]]==['00','01','02','8001','7f']
raw=capture();pos=0;rows=[]
while pos<len(raw):
 base,length=struct.unpack_from('>qi',raw,pos)
 assert length>=49 and raw[pos+16]==2
 stored=struct.unpack_from('>I',raw,pos+17)[0]
 assert stored==crc32c(raw[pos+21:pos+12+length])
 rows.append(base);pos+=12+length
assert pos==len(raw) and rows==[40,41,42,43,44,45] and LAST_STABLE==45
assert ABORTED==[{'producer_id':7,'first_offset':40}]
print('KAFKA_BATCH_WRITER_OK')
if len(sys.argv)>1:print(json.dumps({'batches':rows,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}))
