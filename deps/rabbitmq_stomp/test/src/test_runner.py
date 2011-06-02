#!/usr/bin/env python

import unittest
import sys
import os

def add_deps_to_path():
    deps_dir = os.path.realpath(os.path.join(__file__, "..", "..", "..", "deps"))
    sys.path.append(os.path.join(deps_dir, "stomppy", "stomppy"))

def run_unittests(modules):
    add_deps_to_path()

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

