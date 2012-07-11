#!/usr/bin/env python

from glob import iglob
from gzip import GzipFile
from hashlib import md5
from itertools import chain, ifilter, imap
from optparse import OptionParser
from os.path import basename, join, splitext
try:
    import progressbar
    if progressbar.__version__ < '2.3':
        print "Upgrade to version 2.3 of the 'progressbar' package if " \
                "you're curious how long this will take"
        progressbar = None
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

def index_traces(updates_directory,
                 database_backend,
                 database_name,
                 **database_options):
    index = UpdatesIndexer(
            database_backend, database_name, **database_options)

    tarnames_processed = set(index.tarnames)
    tarnames_unprocessed = filter(lambda f: basename(f) not in tarnames_processed,
                                  iglob(join(updates_directory, '*.tar')))
    number_to_verify = len(tarnames_unprocessed)
    print 'Verifying %d tarfile checksums' % number_to_verify
    if number_to_verify == 0:
        return
    if progressbar is not None:
        vprogress = progressbar.ProgressBar(
                maxval=number_to_verify,
                widgets=[progressbar.SimpleProgress(),
                         progressbar.Bar(),
                         progressbar.Timer()])
    else:
        vprogress = lambda x: x
    tarnames = sorted(filter(verify_checksum, vprogress(tarnames_unprocessed)))

    number_of_tarfiles = len(tarnames)
    print 'Found %d new tar files' % number_of_tarfiles
    if number_of_tarfiles == 0:
        return
    reindex = number_of_tarfiles > 1000
    if progressbar is not None:
        progress = progressbar.ProgressBar(
                maxval=number_of_tarfiles,
                widgets=[progressbar.SimpleProgress(),
                         progressbar.Bar(),
                         progressbar.Timer()])
    else:
        progress = lambda x: x
    index.index(imap(basename, tarnames),
                chain.from_iterable(imap(process_tarfile, progress(tarnames))),
                reindex)

def main():
    usage = 'usage: %prog [options] ' \
            'updates_directory database_backend database_name'
    parser = OptionParser(usage=usage)
    parser.add_option('--postgres-user', action='store',
                      dest='postgres_user',
                      help='Log into Postgres as this user')
    parser.add_option('--postgres-host', action='store',
                      dest='postgres_host',
                      help='Log into Postgres on this host')
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Invalid number of required options')
    database_options = {}
    if options.postgres_host is not None:
        database_options['postgres_host'] = options.postgres_host
    if options.postgres_user is not None:
        database_options['postgres_user'] = options.postgres_user
    index_traces(*args, **database_options)

if __name__ == '__main__':
    main()
