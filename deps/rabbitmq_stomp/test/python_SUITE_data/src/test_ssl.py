#!/usr/bin/env python

import test_runner
import test_util

if __name__ == '__main__':
    modules = ['ssl_lifecycle']
    test_util.ensure_ssl_auth_user()
    test_runner.run_unittests(modules)

