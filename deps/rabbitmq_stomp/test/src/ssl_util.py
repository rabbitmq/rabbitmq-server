import subprocess
import socket
import sys
import os
import os.path

def ensure_ssl_auth_user():
    user = 'O=client,CN=%s' % socket.gethostname()
    rabbitmqctl(['stop_app'])
    rabbitmqctl(['reset'])
    rabbitmqctl(['start_app'])
    rabbitmqctl(['add_user', user, 'foo'])
    rabbitmqctl(['clear_password', user])
    rabbitmqctl(['set_permissions', user, '.*', '.*', '.*'])

def rabbitmqctl(args):
    ctl = os.path.normpath(os.path.join(os.getcwd(), sys.argv[0], '../../../../rabbitmq-server/scripts/rabbitmqctl'))
    cmdline = [ctl, '-n', 'rabbit-test']
    cmdline.extend(args)
    subprocess.check_call(cmdline)
