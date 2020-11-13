#!/usr/bin/env python

import test_runner

if __name__ == '__main__':
    modules = [
        'parsing',
        'errors',
        'lifecycle',
        'ack',
        'amqp_headers',
        'queue_properties',
        'reliability',
        'transactions',
        'x_queue_name',
        'destinations',
        'redelivered',
        'topic_permissions',
        'x_queue_type_quorum'
    ]
    test_runner.run_unittests(modules)
