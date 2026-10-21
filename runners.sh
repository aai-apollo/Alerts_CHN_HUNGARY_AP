#!/bin/bash
cd /home/ec2-user/alert/

# Ensure logs directory exists
mkdir -p logs

# Debug log inside project
echo "[$(date)] Running from cron" >> logs/cron_debug.log

/home/ec2-user/alert/venv/bin/python3 repl_shift_wise.py >> logs/cron_debug.log 2>&1
