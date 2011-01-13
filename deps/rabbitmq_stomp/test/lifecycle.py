import unittest
import stomp
import base
import time

class TestLifecycle(base.BaseTest):

    def test_unsubscribe_destination(self):
        d = "/exchange/amq.fanout"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d)
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send now
        self.listener.reset()
        self.conn.unsubscribe(destination=d)
        self.conn.send("test", destination=d)
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE failed, still receiving messages")

    def test_unsubscribe_id(self):
        ''' Test UNSUBSCRIBE command with id parameter'''
        d = "/exchange/amq.fanout"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, id="test")
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send now
        self.listener.reset()
        self.conn.unsubscribe(id="test")
        self.conn.send("test", destination=d)
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE failed, still receiving messages")

    def test_disconnect(self):
        ''' Run DISCONNECT command '''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

