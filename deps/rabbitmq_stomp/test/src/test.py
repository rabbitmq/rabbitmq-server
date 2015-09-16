#!/usr/bin/env python

import test_runner

if __name__ == '__main__':
    modules = [
        'ack',
        'destinations',
        'errors',
        'lifecycle',
        'parsing',
        'queue_properties',
        'redelivered',
        'reliability',
        'transactions',
    ]
    test_runner.run_unittests(modules)

