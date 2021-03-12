#!/usr/bin/env python3

import test_runner
import test_util

if __name__ == '__main__':
    modules = ['tls_connect_disconnect']
    test_util.ensure_ssl_auth_user()
    test_runner.run_unittests(modules)

