#!/bin/bash
nohup python -u ./GatherByID3.py >> ./log/log &
touch "./log/start-id-$(date "+%Y-%m-%dT%H:%M:%S")"
echo $! > ./log/id
