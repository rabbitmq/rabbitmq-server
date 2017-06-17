#!/usr/bin/env python

import test_runner

if __name__ == '__main__':
    modules = [
        'ack',
        'amqp_headers',
        'destinations',
        'errors',
        'lifecycle',
        'parsing',
        'queue_properties',
        'redelivered',
        'reliability',
        'topic_permissions',
        'transactions',
        'x_queue_name'
    ]
    test_runner.run_unittests(modules)

