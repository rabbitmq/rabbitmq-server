import time
import unittest

import stomp

import testlistener


class TestBasicSend(unittest.TestCase):

    def setUp(self):
        pass

    def testbasic(self):
        conn = stomp.Connection([('127.0.0.2', 61613), ('localhost', 61613)])
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

suite = unittest.TestLoader().loadTestsFromTestCase(TestBasicSend)
unittest.TextTestRunner(verbosity=2).run(suite)
