import subprocess
import sys
import random

failed_events = open('/var/log/authority_failed.txt', 'a')

lines = [line for line in open(sys.argv[1])]
random.shuffle(lines)

for line in lines:
    print "Wiki ", line.strip()
    try:
        print subprocess.check_output(["python", "api_to_database.py", "--wiki-id=%s" % line.strip(), "--processes=64"])
    except:
        failed_events.write(line)

