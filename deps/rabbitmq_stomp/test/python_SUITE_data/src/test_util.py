## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

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
    cmd = ''
    cmd += 'ok = io:format("~n===== Ranch listeners (before stop) =====~n~n~p~n", [ranch:info()]),'
    cmd += '_ = application:stop(rabbitmq_stomp),'
    cmd += 'io:format("~n===== Ranch listeners (after stop) =====~n~n~p~n", [ranch:info()]),'
    if implicit_connect:
        cmd += 'ok = application:set_env(rabbitmq_stomp,implicit_connect,{}),'.format(implicit_connect)
    if default_user:
        cmd += 'ok = application:set_env(rabbitmq_stomp,default_user,{}),'.format(default_user)
    cmd += '_ = application:start(rabbitmq_stomp),'
    cmd += 'io:format("~n===== Ranch listeners (after start) =====~n~n~p~n", [ranch:info()]).'
    rabbitmqctl(['eval', cmd])

def rabbitmqctl(args):
    ctl = os.getenv('RABBITMQCTL')
    cmdline = [ctl, '-n', os.getenv('RABBITMQ_NODENAME')]
    cmdline.extend(args)
    subprocess.check_call(cmdline)
