import unittest
import stomp
import base
import test_util

class TestConnectOptions(base.BaseTest):

    def test_implicit_connect(self):
        ''' Implicit connect with receipt on first command '''
        self.conn.disconnect()
        test_util.enable_implicit_connect()
        listener = base.WaitableListener()
        new_conn = stomp.Connection(user="", passcode="")
        new_conn.set_listener('', listener)

        new_conn.start() # not going to issue connect
        new_conn.subscribe(destination="/topic/implicit", receipt='implicit')

        try:
            self.assertTrue(listener.await(5))
            self.assertEquals(1, len(listener.receipts),
                              'Missing receipt. Likely not connected')
            self.assertEquals('implicit', listener.receipts[0]['headers']['receipt-id'])
        finally:
            new_conn.disconnect()
            test_util.disable_implicit_connect()

    def test_default_user(self):
        ''' Default user connection '''
        self.conn.disconnect()
        new_conn = stomp.Connection(user="", passcode="")
        new_conn.start()
        new_conn.connect()
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()
