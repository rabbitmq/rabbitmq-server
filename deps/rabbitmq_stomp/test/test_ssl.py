#!/usr/bin/env python

import unittest
import sys
import time
import os

import test_runner

class TestSslClient(unittest.TestCase):

    def test_ssl_connect(self):
        ssl_key_file = os.path.abspath("certs/client/key.pem")
        ssl_cert_file = os.path.abspath("certs/client/cert.pem")
        ssl_ca_certs = os.path.abspath("certs/testca/cacert.pem")

        import stomp
        conn = stomp.Connection(user="guest", passcode="guest",
                                host_and_ports = [ ('localhost', 61614) ],
                                use_ssl = True, ssl_key_file = ssl_key_file,
                                ssl_cert_file = ssl_cert_file,
                                ssl_ca_certs = ssl_ca_certs)

        conn.start()
        conn.stop()


if __name__ == '__main__':
    modules = ['test_ssl']
    test_runner.run_unittests(modules)

