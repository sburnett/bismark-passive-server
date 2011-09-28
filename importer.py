import glob
import gzip
from optparse import OptionParser
import os.path
import shutil
import tarfile
import tempfile

import db
import parser

def parse_args():
    usage = 'usage: %prog [options] db_user db_name updates_directory archive_directory'
    parser = OptionParser(usage=usage)
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
        extract_dir = tempfile.mkdtemp(prefix='bismark-passive-')
        tarball.extractall(extract_dir)
        tarball.close()

        updates = []
        update_files = glob.glob(os.path.join(extract_dir, '*.gz'))
        update_files.sort(key=lambda f: os.path.getmtime(f))
        for update_file in update_files:
            print '\t', update_file
            update_handle = gzip.open(update_file, 'rb')
            update_content = update_handle.read()
            update_handle.close()
            updates.append(parser.PassiveUpdate(update_content))

        shutil.rmtree(extract_dir)
        shutil.move(filename, args['archive_directory'])

        updates.sort(key=lambda u: (u.creation_time, u.sequence_number))
        for update in updates:
            database.import_update(update)

if __name__ == '__main__':
    main()
