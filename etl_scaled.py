import subprocess
import sys
from argparse import ArgumentParser, FileType
from boto import connect_s3, connect_ec2
from boto.utils import get_instance_metadata


class Unbuffered:

    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--infile', dest='infile', type=FileType('r'))
    ap.add_argument('--s3file', dest='s3file')
    ap.add_argument('--overwrite', dest='overwrite', action='store_true', default=False)
    ap.add_argument('--die-on-complete', dest='die_on_complete', action='store_true', default=False)
    return ap.parse_args()


def main():
    sys.stdout = Unbuffered(sys.stdout)
    bucket = connect_s3().get_bucket('nlp-data')
    failed_events = open('/var/log/authority_failed.txt', 'a')

    args = get_args()
    if args.s3file:
        fname = args.s3file.split('/')[-1]
        bucket.get_key(args.s3file).get_file(open(fname, 'w'))
        fl = open(fname, 'r')
    else:
        fl = args.infile

    for line in fl:
        key = bucket.get_key(key_name='service_responses/%s/WikiAuthorityService.get' % line.strip())
        if (not args.overwrite) and (key is not None and key.exists()):
            print "Key exists for", line.strip()
            continue
        print "Wiki ", line.strip()
        try:
            print subprocess.call("python api_to_database.py --wiki-id=%s --processes=64" % line.strip(), shell=True)
        except Exception as e:
            print e
            failed_events.write(line)

    if args.die_on_complete:
        current_id = get_instance_metadata()['instance-id']
        ec2_conn = connect_ec2()
        ec2_conn.terminate_instances([current_id])


if __name__ == '__main__':
    main()