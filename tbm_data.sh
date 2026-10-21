#!/bin/bash
cd /home/ec2-user/Alerts/v1/

# Ensure logs directory exists
mkdir -p logs

# Debug log inside project
echo "[$(date)] Running from cron" >> logs/cron_debug.log

/home/ec2-user/Alerts/v1/venv/bin/python3 tbm_data.py >> logs/cron_debug.log 2>&1
