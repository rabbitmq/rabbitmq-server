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

def enable_implicit_connect():
    switch_config(implicit_connect='true', default_user='[{login, "guest"}, {passcode, "guest"}]')

def disable_implicit_connect():
    switch_config(implicit_connect='false', default_user='[]')

def enable_default_user():
    switch_config(default_user='[{login, "guest"}, {passcode, "guest"}]')

def disable_default_user():
    switch_config(default_user='[]')

def switch_config(implicit_connect='', default_user=''):
    cmd = 'application:stop(rabbitmq_stomp),'
    if implicit_connect:
        cmd += 'application:set_env(rabbitmq_stomp,implicit_connect,' + implicit_connect + '),'
    if default_user:
        cmd += 'application:set_env(rabbitmq_stomp,default_user,' + default_user + '),'
    cmd += 'application:start(rabbitmq_stomp).'
    rabbitmqctl(['eval', cmd])

def rabbitmqctl(args):
    ctl = os.path.normpath(os.path.join(os.getcwd(), sys.argv[0], '../../../../rabbitmq-server/scripts/rabbitmqctl'))
    cmdline = [ctl, '-n', 'rabbit-test']
    cmdline.extend(args)
    subprocess.check_call(cmdline)

