#!/usr/bin/env python 

import glob
from gzip import GzipFile
from hashlib import md5
import os
import os.path
import StringIO
import sys
import tarfile

def md5sum(filename):
    m = md5()
    file = open(filename)
    while True:
        data = file.read(128)
        if not data:
            break
        m.update(data)
    return m.hexdigest()

def anonymize_update(tarfilename, destination_directory):
    # extract the router ID from file name and create an MD5 hash
    router_id = os.path.basename(tarfilename).split('_', 1)[0]
    m = md5()
    m.update(router_id)
    hashed_router_id = m.hexdigest()

    output_tarfilename \
            = '%s_%s' % (hashed_router_id,
                         os.path.basename(tarfilename).split('_', 1)[1])
    output_path = os.path.join(destination_directory, output_tarfilename)

    if os.path.exists(output_path):
        return

    # Read a tar file and output an anonymized tar file
    tarball = tarfile.open(tarfilename, 'r')
    outtarball = tarfile.open(output_path, 'w')
    for tarmember in tarball:
        # get the .gz file
        tarhandle = tarball.extractfile(tarmember.name)
        logfile = GzipFile(fileobj=tarhandle)
        outlogfile = StringIO.StringIO()
        for line in logfile:
            if line.strip() == 'UNANONYMIZED':
                continue
            if router_id in line:
                outlogfile.write(line.replace(router_id, hashed_router_id))
            else:
                outlogfile.write(line)
        outlogfile.seek(0) 

        # another buffer to write the .gz version of above log file
        outtarbuffer = StringIO.StringIO()
        outtarhandle = GzipFile(mode='w', fileobj=outtarbuffer)

        # write compressed lines to the .gz file
        outtarhandle.writelines(outlogfile)
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
    new_md5checksum = md5sum(output_path)
    final_filename = '%s_%s.tar' % ('_'.join(os.path.splitext(output_tarfilename)[0].split('_')[:3]),
                                    new_md5checksum)
    os.rename(os.path.join(destination_directory, output_tarfilename),
              os.path.join(destination_directory, final_filename))

def main():
    if len(sys.argv) != 3:
        print 'usage: %s <raw traces directory> <anonymized traces directory>'
        sys.exit(1)
    source_directory = sys.argv[1]
    destination_directory = sys.argv[2]
    for filename in glob.glob(os.path.join(source_directory, '*.tar')):
        print filename
        anonymize_update(filename, destination_directory)

if __name__ == '__main__':
    main()
