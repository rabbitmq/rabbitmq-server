#!/usr/bin/env python

import test_runner

if __name__ == '__main__':
    modules = [
        'parsing',
        'destinations',
        'lifecycle',
        'transactions',
        'ack',
        'errors',
        'reliability',
        'queue_properties',
    ]
    test_runner.run_unittests(modules)

