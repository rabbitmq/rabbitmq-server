#!/usr/bin/env python3

## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
##

import unittest
import sys
import os

def run_unittests(modules):
    suite = unittest.TestSuite()
    for m in modules:
        mod = __import__(m)
        for name in dir(mod):
            obj = getattr(mod, name)
            if name.startswith("Test") and issubclass(obj, unittest.TestCase):
                suite.addTest(unittest.TestLoader().loadTestsFromTestCase(obj))

    ts = unittest.TextTestRunner().run(unittest.TestSuite(suite))
    if ts.errors or ts.failures:
        sys.exit(1)

