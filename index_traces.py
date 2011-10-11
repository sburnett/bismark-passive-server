#!/usr/bin/env python

import datetime
import errno
import glob
import gzip
from optparse import OptionParser
import os
import os.path
import shutil
import tarfile

import parser

def parse_args():
    usage = 'usage: %prog [options] updates_directory index_directory archive_directory'
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Missing required option')
    mandatory = { 'updates_directory': args[0],
                  'index_directory': args[1],
                  'archive_directory': args[2] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    filenames = sorted(glob.glob(os.path.join(args['updates_directory'], '*.tar')))
    for filename in filenames:
        print 'Processing', filename
        tarball = tarfile.open(filename)
        tarmembers = tarball.getmembers()
        tarmembers.sort(key=lambda m: m.mtime)

        updates = []
        for tarmember in tarmembers:
            print '\t', tarmember.name
            tarhandle = tarball.extractfile(tarmember.name)
            update_content = gzip.GzipFile(fileobj=tarhandle).read()
            update = parser.PassiveUpdate(update_content, onlyheaders=True)
            if update.anonymized:
                signature = update.anonymization_signature
            else:
                signature = 'unanonymized'
            index_path = os.path.join(args['index_directory'],
                                      update.bismark_id,
                                      signature,
                                      str(update.creation_time))
            try:
                os.makedirs(index_path)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            tarball.extract(tarmember, index_path)

        shutil.move(filename, args['archive_directory'])

if __name__ == '__main__':
    main()
