#!/usr/bin/env bash
backup_location="/opt/mongobackups"

for entry in  "$backup_location"/*
do
        filename=$(basename $entry)
        extension=$(echo $filename | tail -c 5)
        filename_wo_ext=$(echo $filename | cut -f 1 -d '.')

        if [ $filename_wo_ext = "" ]
                then
                        break
        fi

        if [ $extension = "bson" ]
                then

                mongorestore -d dvpdb -c $filename_wo_ext $backup_location/$filename
                rm "$filename_wo_ext".*
        fi
#mongorestore -d dvpdb -c DashboardSesion /opt/mongobackup/DashboardSessions.bson
done
