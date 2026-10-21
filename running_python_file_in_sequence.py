import subprocess

venv_python = "/home/ec2-user/alert/venv/bin/python3"

subprocess.run([venv_python, "/home/ec2-user/alert/hungary_alert.py"])
subprocess.run([venv_python, "/home/ec2-user/alert/hungary_mail.py"])
