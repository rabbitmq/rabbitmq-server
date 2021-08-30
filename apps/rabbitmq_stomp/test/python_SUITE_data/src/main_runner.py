#!/usr/bin/env python3

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
