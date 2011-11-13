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
    m.update(open(filename, 'r').read())
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
        outtarbuffer = StringIO.StringIO()
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
    new_md5checksum = md5sum(output_path)
    final_filename = '%s_%s.tar' % ('_'.join(os.path.splitext(output_tarfilename)[0].split('_')[:3]),
                                    new_md5checksum)
    os.rename(os.path.join(destination_directory, output_tarfilename),
              os.path.join(destination_directory, final_filename))
