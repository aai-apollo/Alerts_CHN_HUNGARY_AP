import subprocess

venv_python = "/home/ec2-user/alert/venv/bin/python3"

subprocess.run([venv_python, "/home/ec2-user/alert/hungary_repl.py"])
subprocess.run([venv_python, "/home/ec2-user/alert/hungary_repl_mail.py"])
