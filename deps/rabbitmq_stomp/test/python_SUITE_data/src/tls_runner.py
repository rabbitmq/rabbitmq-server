#!/usr/bin/env python3

## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import test_runner
import test_util

if __name__ == '__main__':
    modules = ['tls_connect_disconnect']
    test_util.ensure_ssl_auth_user()
    test_runner.run_unittests(modules)

