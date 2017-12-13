#!/bin/bash
nohup python -u ./stream.py >> ./log/log &
touch "./log/start $(date)"
