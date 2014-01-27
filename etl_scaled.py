import subprocess
import sys
import random
from boto import connect_s3

c = connect_s3()
b = c.get_bucket('nlp-data')

failed_events = open('/var/log/authority_failed.txt', 'a')

lines = [line for line in open(sys.argv[1])]
random.shuffle(lines)

class Unbuffered:
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

sys.stdout=Unbuffered(sys.stdout)


for line in lines:
    key = b.get_key(key_name='service_responses/%s/WikiAuthorityService.get' % line.strip())
    if key is not None and key.exists():
        print "Key exists for", line.strip()
        continue
    print "Wiki ", line.strip()
    try:
        print subprocess.call("python api_to_database.py --wiki-id=%s --processes=64" % line.strip(), shell=True)
    except Exception as e:
        print e
        failed_events.write(line)

