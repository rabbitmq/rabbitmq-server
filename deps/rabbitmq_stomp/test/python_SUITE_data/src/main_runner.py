#!/usr/bin/env python3

import sys
import subprocess

# implement pip as a subprocess:
subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                       'stomp.py==8.1.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                       'pika==1.1.0'])

import test_runner

if __name__ == '__main__':
    modules = [
        'parsing',
        'errors',
        'connect_disconnect',
        'ack',
        'amqp_headers',
        'queue_properties',
        'reliability',
        'transactions',
        'x_queue_name',
        'destinations',
        'redelivered',
        'topic_permissions',
        'unsubscribe',
        'x_queue_type_quorum',
        'x_queue_type_stream'
    ]
    test_runner.run_unittests(modules)
