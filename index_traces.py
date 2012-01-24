#!/usr/bin/env python

from glob import iglob
from gzip import GzipFile
from hashlib import md5
from itertools import chain, ifilter, imap
from optparse import OptionParser
from os.path import basename, join, splitext
try:
    import progressbar
except ImportError:
    print "Install the 'progressbar' package if " \
            "you're curious how long this will take"
    progressbar = None
import tarfile

from update_parser import PassiveUpdate
from updates_index import UpdatesIndexer

def verify_checksum(tarname):
    try:
        true_sum = splitext(basename(tarname))[0].split('_')[3]
    except IndexError:
        true_sum = None
    if true_sum is not None:
        hasher = md5()
        hasher.update(open(tarname, 'r').read())
        if hasher.hexdigest() != true_sum:
            print 'skipping', tarname, '(invalid hash)'
            return False
    return True

def process_tarfile(tarname):
    tarball = tarfile.open(tarname, 'r')
    def extract_update(tarmember):
        tarhandle = tarball.extractfile(tarmember.name)
        try:
            return GzipFile(fileobj=tarhandle).read()
        except IOError:
            print 'skipping', tarname, '(IO Error)'
            return None
    update_contents = ifilter(lambda el: el is not None,
                              imap(extract_update,
                                   tarball.getmembers()))
    return imap(PassiveUpdate, update_contents)

def index_traces(updates_directory, index_filename):
    index = UpdatesIndexer(index_filename)
    tarnames_processed = set(index.tarnames)
    tarnames_unprocessed = filter(lambda f: basename(f) not in tarnames_processed,
                                  iglob(join(updates_directory, '*.tar')))
    number_to_verify = len(tarnames_unprocessed)
    print 'Verifying %d tarfile checksums' % number_to_verify
    if progressbar is not None:
        vprogress = progressbar.ProgressBar(
                maxval=number_to_verify,
                widgets=[progressbar.SimpleProgress(),
                         progressbar.Bar(),
                         progressbar.Timer()])
        tarnames = filter(verify_checksum, vprogress(tarnames_unprocessed))
    else:
        tarnames = filter(verify_checksum, tarnames_unprocessed)

    number_of_tarfiles = len(tarnames)
    print 'Found %d new tar files' % number_of_tarfiles
    reindex = number_of_tarfiles > 1000
    if progressbar is not None:
        progress = progressbar.ProgressBar(
                maxval=number_of_tarfiles,
                widgets=[progressbar.SimpleProgress(),
                         progressbar.Bar(),
                         progressbar.Timer()])
        index.index(imap(basename, tarnames),
                    chain.from_iterable(imap(process_tarfile, progress(tarnames))),
                    reindex)
    else:
        index.index(imap(basename, tarnames),
                    chain.from_iterable(imap(process_tarfile, tarnames)),
                    reindex)

def main():
    usage = 'usage: %prog [options] updates_directory index_filename'
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.error('Invalid number of required options')
    index_traces(*args)

if __name__ == '__main__':
    main()
