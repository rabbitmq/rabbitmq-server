#!/usr/bin/env python

import test_runner
import ssl_util

if __name__ == '__main__':
    modules = ['ssl_lifecycle']
    ssl_util.ensure_ssl_auth_user()
    test_runner.run_unittests(modules)

