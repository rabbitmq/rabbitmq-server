import unittest
import stomp
import base
import time
import os
import threading

import test_util

class TestImplicitConnect(base.BaseTest):
    """
    Relies on implicit connect being enabled on the node
    """

    def test_implicit_connect(self):
        ''' Implicit connect with receipt on first command '''
        self.conn.disconnect()
        test_util.enable_implicit_connect()
        listener = base.WaitableListener()
        new_conn = stomp.Connection(host_and_ports=[('localhost', int(os.environ["STOMP_PORT"]))])
        new_conn.set_listener('', listener)

        new_conn.transport.start()

        self.subscribe_dest(new_conn, "/topic/implicit", 'sub_implicit',
                            receipt='implicit')

        try:
            self.assertTrue(listener.wait(5))
            self.assertEqual(1, len(listener.receipts),
                              'Missing receipt. Likely not connected')
            self.assertEqual('implicit', listener.receipts[0]['headers']['receipt-id'])
        finally:
            new_conn.disconnect()
            test_util.disable_implicit_connect()


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)