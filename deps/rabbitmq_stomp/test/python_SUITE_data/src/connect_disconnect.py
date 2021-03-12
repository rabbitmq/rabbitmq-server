## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import unittest
import stomp
import base
import time
import os
import threading

import test_util

class TestConnectDisconnect(base.BaseTest):
    def test_connect_version_1_0(self):
        ''' Test CONNECT with version 1.0'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.0")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()

    def test_connect_version_1_1(self):
        ''' Test CONNECT with version 1.1'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.1")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()

    def test_connect_version_1_2(self):
        ''' Test CONNECT with version 1.2'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.2")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()

    def test_default_user(self):
        ''' Default user connection '''
        self.conn.disconnect()
        test_util.enable_default_user()
        listener = base.WaitableListener()
        new_conn = stomp.Connection(host_and_ports=[('localhost', int(os.environ["STOMP_PORT"]))])
        new_conn.set_listener('', listener)
        new_conn.connect()
        try:
            self.assertFalse(listener.wait(3)) # no error back
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()
            test_util.disable_default_user()


    def test_unsupported_version(self):
        ''' Test unsupported version on CONNECT command'''
        self.bad_connect("Supported versions are 1.0,1.1,1.2\n", version='100.1')

    def test_bad_username(self):
        ''' Test bad username'''
        self.bad_connect("Access refused for user 'gust'\n", user='gust')

    def test_bad_password(self):
        ''' Test bad password'''
        self.bad_connect("Access refused for user 'guest'\n", passcode='gust')

    def test_bad_vhost(self):
        ''' Test bad virtual host'''
        self.bad_connect("Virtual host '//' access denied", version='1.1', vhost='//')

    def bad_connect(self, expected, user='guest', passcode='guest', **kwargs):
        self.conn.disconnect()
        new_conn = self.create_connection_obj(**kwargs)
        listener = base.WaitableListener()
        new_conn.set_listener('', listener)
        try:
            new_conn.connect(user, passcode)
            self.assertTrue(listener.wait())
            self.assertEqual(expected, listener.errors[0]['message'])
        finally:
            if new_conn.is_connected():
                new_conn.disconnect()

    def test_bad_header_on_send(self):
        ''' Test disallowed header on SEND '''
        self.listener.reset(1)
        self.conn.send_frame("SEND", {"destination":"a", "message-id":"1"})
        self.assertTrue(self.listener.wait())
        self.assertEqual(1, len(self.listener.errors))
        errorReceived = self.listener.errors[0]
        self.assertEqual("Invalid header", errorReceived['headers']['message'])
        self.assertEqual("'message-id' is not allowed on 'SEND'.\n", errorReceived['message'])

    def test_send_recv_header(self):
        ''' Test sending a custom header and receiving it back '''
        dest = '/queue/custom-header'
        hdrs = {'x-custom-header-1': 'value1',
                'x-custom-header-2': 'value2',
                'custom-header-3': 'value3'}
        self.listener.reset(1)
        recv_hdrs = self.simple_test_send_rec(dest, headers=hdrs)
        self.assertEqual('value1', recv_hdrs['x-custom-header-1'])
        self.assertEqual('value2', recv_hdrs['x-custom-header-2'])
        self.assertEqual('value3', recv_hdrs['custom-header-3'])

    def test_disconnect(self):
        ''' Test DISCONNECT command'''
        self.conn.disconnect()
        # Note: with modern-ish stomp.py versions, connection does not transition
        #       to the disconnected state immediately, and asserting on it in this test
        #       without a receipt makes no sense

    def test_disconnect_with_receipt(self):
        ''' Test the DISCONNECT command with receipts '''
        time.sleep(3)
        self.listener.reset(1)
        self.conn.send_frame("DISCONNECT", {"receipt": "test"})
        self.assertTrue(self.listener.wait())
        self.assertEqual(1, len(self.listener.receipts))
        receiptReceived = self.listener.receipts[0]['headers']['receipt-id']
        self.assertEqual("test", receiptReceived
                         , "Wrong receipt received: '" + receiptReceived + "'")

if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)