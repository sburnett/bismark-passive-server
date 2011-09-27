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
    usage = 'usage: %prog [options] db_user db_name updates_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-d', '--updates-directory', dest='directory')
    parser.add_option('-u', '--db-user', dest='db_user')
    parser.add_option('-n', '--db-name', dest='db_name')
    options, args = parser.parse_args()
    if len(args) != 3:
        parser.error('Missing required option')
    return options, args

def main():
    (options, args) = parse_args()
    database = db.BismarkPassiveDatabase(args[0], args[1])
    filenames = sorted(glob.glob(os.path.join(args[2], '*.tar')))
    for filename in filenames:
        tarball = tarfile.open(filename)
        extract_dir = tempfile.mkdtemp(prefix='bismark-passive-')
        tarball.extractall(extract_dir)
        tarball.close()

        updates = []
        for update_file in glob.glob(os.path.join(extract_dir, '*.gz')):
            update_handle = gzip.open(update_file, 'rb')
            update_content = update_handle.read()
            update_handle.close()
            updates.append(parser.PassiveUpdate(update_content))

        updates.sort(key=lambda u: (u.creation_time, u.sequence_number))
        for update in updates:
            database.import_update(update)

        shutil.rmtree(extract_dir)

if __name__ == '__main__':
    main()
