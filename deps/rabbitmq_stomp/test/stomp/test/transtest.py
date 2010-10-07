import time
import unittest

import stomp

import testlistener


class TestTrans(unittest.TestCase):

    def setUp(self):
        conn = stomp.Connection([('127.0.0.2', 61613), ('localhost', 61613)])
        listener = testlistener.TestListener()
        conn.set_listener('', listener)
        conn.start()
        conn.connect(wait=True)
        self.conn = conn
        self.listener = listener
        
    def tearDown(self):
        self.conn.disconnect()

    def testcommit(self):
        self.conn.subscribe(destination='/queue/test', ack='auto')
        trans_id = self.conn.begin()
        self.conn.send('this is a test1', destination='/queue/test', transaction=trans_id)
        self.conn.send('this is a test2', destination='/queue/test', transaction=trans_id)
        self.conn.send('this is a test3', destination='/queue/test', transaction=trans_id)

        time.sleep(3)
        
        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.messages == 0, 'should not have received any messages')
        
        self.conn.commit(transaction = trans_id)
        time.sleep(3)
        
        self.assert_(self.listener.messages == 3, 'should have received 3 messages')
        self.assert_(self.listener.errors == 0, 'should not have received any errors')

    def testabort(self):
        self.conn.subscribe(destination='/queue/test', ack='auto')
        trans_id = self.conn.begin()
        self.conn.send('this is a test1', destination='/queue/test', transaction=trans_id)
        self.conn.send('this is a test2', destination='/queue/test', transaction=trans_id)
        self.conn.send('this is a test3', destination='/queue/test', transaction=trans_id)

        time.sleep(3)
        
        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.messages == 0, 'should not have received any messages')
        
        self.conn.abort(transaction = trans_id)
        time.sleep(3)
        
        self.assert_(self.listener.messages == 0, 'should not have received any messages')
        self.assert_(self.listener.errors == 0, 'should not have received any errors')
        
suite = unittest.TestLoader().loadTestsFromTestCase(TestTrans)
unittest.TextTestRunner(verbosity=2).run(suite)
