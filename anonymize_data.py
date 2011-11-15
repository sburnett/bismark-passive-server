#!/usr/bin/env python 
# Testing commit permissions

from cStringIO import StringIO
from glob import glob
from gzip import GzipFile
from hashlib import md5
from multiprocessing import Pool
from optparse import OptionParser
from os.path import basename, join, splitext
import sys
import tarfile

def anonymize_update(tarfilename, destination_directory, check_destination):
    # extract the router ID from file name and create an MD5 hash
    router_id = basename(tarfilename).split('_', 1)[0]
    m = md5()
    m.update(router_id)
    hashed_router_id = m.hexdigest()

    output_pat = '%s_%s*.tar' \
            % (hashed_router_id,
               '_'.join(splitext(basename(tarfilename))[0].split('_', 3)[1:3]))
    existing_filenames = glob(join(destination_directory, output_pat))
    if len(existing_filenames) > 0:
        assert len(existing_filenames) == 1
        if check_destination:
            true_sum = splitext(
                    basename(existing_filenames[0]))[0].split('_')[3]
            hasher = md5()
            hasher.update(open(existing_filenames[0], 'r').read())
            if hasher.hexdigest() == true_sum:
                sys.stdout.write('_')
                sys.stdout.flush()
                return
            else:
                sys.stdout.write('!')
                sys.stdout.flush()
        else:
            sys.stdout.write('_')
            sys.stdout.flush()
            return
    try:
        true_sum = splitext(basename(tarfilename))[0].split('_')[3]
    except IndexError:
        true_sum = None
    if true_sum is not None:
        hasher = md5()
        hasher.update(open(tarfilename, 'r').read())
        if hasher.hexdigest() != true_sum:
            sys.stdout.write('?')
            sys.stdout.flush()
            return
    sys.stdout.write('.')
    sys.stdout.flush()

    # Read a tar file and output an anonymized tar file
    tarball = tarfile.open(tarfilename, 'r')
    outfile = StringIO()
    outtarball = tarfile.open(fileobj=outfile, mode='w')
    for tarmember in tarball:
        # get the .gz file
        tarhandle = tarball.extractfile(tarmember.name)
        logfile = GzipFile(fileobj=tarhandle)
        outtarbuffer = StringIO()
        outtarhandle = GzipFile(mode='w', fileobj=outtarbuffer)
        unanonymized = False
        for line in logfile:
            if line.startswith('UNANONYMIZED'):
                unanonymized = True
                break
            outtarhandle.write(line.replace(router_id, hashed_router_id))
        if unanonymized:
            continue

        # write compressed lines to the .gz file
        outtarhandle.close()
        outtarsize = outtarbuffer.tell() # size of the final buffer
        outtarbuffer.seek(0)

        # anonymize the router id from the gzip file as well
        outgzname = '%s-%s' % (hashed_router_id,
                               tarmember.name.split('-', 1)[1])
        outtarmember = tarfile.TarInfo(name=outgzname)
        outtarmember.size = outtarsize
        outtarmember.mtime = tarmember.mtime
        outtarmember.mode = tarmember.mode
        outtarmember.type = tarmember.type

        # add anonymized .gz file to tarball 
        outtarball.addfile(outtarmember, outtarbuffer)
    outtarball.close()

    # Change the md5sum in the name of the file
    m = md5()
    m.update(outfile.getvalue())
    new_md5checksum = m.hexdigest()
    output_tarfilename = '%s_%s_%s.tar' \
            % (hashed_router_id,
               '_'.join(splitext(basename(tarfilename))[0].split('_', 3)[1:3]),
               new_md5checksum)
    output_path = join(destination_directory, output_tarfilename)
    open(output_path, 'w').write(outfile.getvalue())
    return output_path

def parse_args():
    usage = 'usage: %prog [options] source_directory anonymized_directory'
    parser = OptionParser(usage=usage)
    parser.add_option('-w', '--workers', type='int', action='store',
                      dest='workers', default=4,
                      help='Maximum number of worker threads to use')
    parser.add_option('-c', '--check-destination-sums', action='store_true',
                      dest='check_destination', default=False,
                      help='Verify destination checksums before skipping')
    options, args = parser.parse_args()
    if len(args) != 2:
        parser.error('Missing required option')
    mandatory = { 'source_directory': args[0],
                  'anonymized_directory': args[1] }
    return options, mandatory

def main():
    (options, args) = parse_args()
    pool = Pool(processes=options.workers)
    results = []
    for tarname in glob(join(args['source_directory'], '*.tar')):
        pool.apply_async(anonymize_update,
                         (tarname,
                          args['anonymized_directory'],
                          options.check_destination))
    pool.close()
    pool.join()
    print

if __name__ == '__main__':
    main()
