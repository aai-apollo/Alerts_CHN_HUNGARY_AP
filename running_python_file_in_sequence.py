import subprocess

venv_python = "/home/ec2-user/Alerts/v1/venv/bin/python3"

subprocess.run([venv_python, "/home/ec2-user/Alerts/v1/app.py"])
subprocess.run([venv_python, "/home/ec2-user/Alerts/v1/trigger_mail.py"])
# subprocess.run([venv_python, "/home/ec2-user/Alerts/v1/alert_trigger_param.py"])
