import time
import unittest

import stomp

import testlistener


class TestSSLSend(unittest.TestCase):

    def setUp(self):
        pass

    def testsslbasic(self):
        conn = stomp.Connection([('127.0.0.1', 61612), ('localhost', 61612)], use_ssl = True)
        listener = testlistener.TestListener()
        conn.set_listener('', listener)
        conn.start()
        conn.connect(wait=True)
        conn.subscribe(destination='/queue/test', ack='auto')

        conn.send('this is a test', destination='/queue/test')

        time.sleep(3)
        conn.disconnect()

        self.assert_(listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(listener.messages == 1, 'should have received 1 message')
        self.assert_(listener.errors == 0, 'should not have received any errors')


suite = unittest.TestLoader().loadTestsFromTestCase(TestSSLSend)
unittest.TextTestRunner(verbosity=2).run(suite)
