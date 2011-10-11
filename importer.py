import glob
import gzip
from optparse import OptionParser
import os.path
import shutil
import tarfile

import db
import parser

def parse_args():
    usage = 'usage: %prog [options] db_user db_name updates_directory archive_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-n', '--fake', dest='fake',
                      action='store_true', default=False,
                      help="Don't actually import files")
    options, args = parser.parse_args()
    if len(args) != 4:
        parser.error('Missing required option')
    mandatory = { 'db_user': args[0],
                  'db_name': args[1],
                  'updates_directory': args[2],
                  'archive_directory': args[3] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    database = db.BismarkPassiveDatabase(args['db_user'], args['db_name'])
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
            updates.append(parser.PassiveUpdate(update_content))

        if not options.fake:
            shutil.move(filename, args['archive_directory'])
            updates.sort(key=lambda u: (u.creation_time, u.sequence_number))
            for update in updates:
                print 'Importing %d packets' % len(update.packet_series)
                database.import_update(update)

if __name__ == '__main__':
    main()
