import subprocess

venv_python = "/home/ec2-user/alert_ap/venv/bin/python3.9"

subprocess.run([venv_python, "/home/ec2-user/alert_ap/app.py"])
subprocess.run([venv_python, "/home/ec2-user/alert_ap/trigger_mail.py"])
