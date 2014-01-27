import subprocess
import sys
import random
from boto import connect_s3

c = connect_s3()
b = c.get_bucket('nlp-data')

failed_events = open('/var/log/authority_failed.txt', 'a')

lines = [line for line in open(sys.argv[1])]
random.shuffle(lines)

for line in lines:
    key = b.get_key(key_name='service_responses/%s/WikiAuthorityService.get' % line.strip())
    if key is not None:
        continue
    print "Wiki ", line.strip()
    try:
        print subprocess.check_output(["python", "api_to_database.py", "--wiki-id=%s" % line.strip(), "--processes=64"])
    except:
        failed_events.write(line)

