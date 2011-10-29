#!/usr/bin/env python

from glob import glob
from gzip import GzipFile
from optparse import OptionParser
from os.path import basename, join
import tarfile

from update_parser import PassiveUpdate
from updates_index import UpdatesIndex

def index_traces(updates_directory, index_filename):
    index = UpdatesIndex(index_filename)
    tarnames_processed = index.tarnames
    for tarname in glob(join(updates_directory, '*.tar')):
        if basename(tarname) not in tarnames_processed:
            print 'Indexing', tarname
            tarball = tarfile.open(tarname, 'r')
            for tarmember in tarball.getmembers():
                print ' ', tarmember.name
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
                            update.sequence_number)
