import unittest
import stomp
import base
import time

class TestLifecycle(base.BaseTest):

    def test_unsubscribe_exchange_destination(self):
        ''' Test UNSUBSCRIBE command with exchange'''
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
                         "UNSUBSCRIBE exchange failed, still receiving messages")

    def test_unsubscribe_queue_destination(self):
        ''' Test UNSUBSCRIBE command with queue'''
        d = "/queue/unsub01"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d)
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and try send again
        self.listener.reset()
        self.conn.unsubscribe(destination=d)
        self.conn.send("test", destination=d)
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE queue failed, still receiving messages")

    def test_unsubscribe_exchange_id(self):
        ''' Test UNSUBSCRIBE command with exchange by id'''
        d = "/exchange/amq.fanout"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, id="exchid")
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send now
        self.listener.reset()
        self.conn.unsubscribe(id="exchid")
        self.conn.send("test", destination=d)
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE exchange by id failed, still receiving messages")

    def test_unsubscribe_queue_id(self):
        ''' Test UNSUBSCRIBE command with queue by id'''
        d = "/queue/unsub02"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, id="queid")
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send now
        self.listener.reset()
        self.conn.unsubscribe(id="queid")
        self.conn.send("test", destination=d)
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE queue by id failed, still receiving messages")

    def test_disconnect(self):
        ''' Run DISCONNECT command '''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

