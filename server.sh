#!/bin/bash

# start server
if [ $1 = 'start' ]; then
    nohup /usr/bin/python3 server.py -d /mnt/volume_ams3_01/ > nohup.out &
fi

# stop server
if [ $1 = 'stop' ]; then
    kill $(ps -ef | grep 'server.py' | awk '{print $2}' | head -1)
fi