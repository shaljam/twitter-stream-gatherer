#!/bin/bash
nohup python -u ./stream.py >> ./log/log &
touch "./log/start-$(date "+%Y-%m-%dT%H:%M:%S")"
echo $! > ./log/id
