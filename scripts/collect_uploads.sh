#!/bin/bash

UPLOADS_DIR=$1
UPDATES_DIR=$2

if [ ! -d "$UPLOADS_DIR" -o ! -d "$UPDATES_DIR" ]; then
    echo "Usage: $0 <uploads_dir> <updates_dir>"
    exit 1
fi

shopt -s nullglob

for node_dir in $UPLOADS_DIR/*; do
    if [ -d $node_dir ]; then
        cd $node_dir
        node_id=`basename $node_dir`
        UPDATE_FILES=(*.gz)
        if [ ${#UPDATE_FILES[*]} != 0 ]; then
            sleep 3
            DESTNAME=$UPDATES_DIR/${node_id}_$(date +%Y-%m-%d_%H-%M-%S).tar
            if [ -f $DESTNAME ]; then
                echo "Tarfile $DESTNAME already exists!"
                exit 1
            fi
            TEMPNAME=`mktemp /tmp/tmp-bismark-passive-upload.XXXXXXXXX`
            if [ $? -ne 0 ]; then
                echo "Cannot create temporary directory"
                exit 1
            fi
            tar cf $TEMPNAME ${UPDATE_FILES[*]} \
                && mv $TEMPNAME $DESTNAME \
                && chgrp bismark-passive $DESTNAME \
                && chmod 640 $DESTNAME \
                && rm ${UPDATE_FILES[*]}
        fi
    fi
done

SSH_AUTH_SOCK= rsync -az -e "ssh -i /data/users/sburnett/.ssh/id_rsa_backups" $UPDATES_DIR/ dp5.gtnoise.net:
