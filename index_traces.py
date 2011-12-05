#!/usr/bin/env python

from glob import glob
from gzip import GzipFile
from hashlib import md5
from optparse import OptionParser
from os.path import basename, join, splitext
import tarfile

from update_parser import PassiveUpdate
from updates_index import UpdatesIndex

def index_traces(updates_directory, index_filename):
    index = UpdatesIndex(index_filename)
    tarnames_processed = index.tarnames
    for tarname in glob(join(updates_directory, '*.tar')):
        if basename(tarname) not in tarnames_processed:
            try:
                true_sum = splitext(basename(tarname))[0].split('_')[3]
            except IndexError:
                true_sum = None
            if true_sum is not None:
                hasher = md5()
                hasher.update(open(tarname, 'r').read())
                if hasher.hexdigest() != true_sum:
                    print 'skipping', tarname, '(invalid hash)'
                    continue

            tarball = tarfile.open(tarname, 'r')
            for tarmember in tarball.getmembers():
                tarhandle = tarball.extractfile(tarmember.name)
                update_content = GzipFile(fileobj=tarhandle).read()
                update = PassiveUpdate(update_content, onlyheaders=True)
                if update.anonymized:
                    signature = update.anonymization_signature
                else:
                    signature = 'unanonymized'
                index.index(basename(tarname),
                            tarmember.name,
                            update.bismark_id,
                            signature,
                            update.creation_time,
                            update.sequence_number,
                            len(update_content))
    index.finalize_indexing()
