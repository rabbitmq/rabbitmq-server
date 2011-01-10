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

        # unsubscribe and send now
        self.listener.reset()
        self.conn.unsubscribe(id="test")
        self.conn.send("test", destination=d)
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE failed, still receiving messages")

    def test_connect_version_1_1(self):
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.1,1.0")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()

    def test_heartbeat_disconnects_client(self):
        self.conn.disconnect()
        new_conn = self.create_connection(heartbeat="1500,0")
        try:
            self.assertTrue(new_conn.is_connected())
            time.sleep(1)
            self.assertTrue(new_conn.is_connected())
            time.sleep(3)
            self.assertFalse(new_conn.is_connected())
        finally:
            if new_conn.is_connected():
                new_conn.disconnect()



    def test_unsupported_version(self):
        self.conn.disconnect()
        new_conn = stomp.Connection(user="guest",
                                    passcode="guest",
                                    version="100.1")
        listener = base.WaitableListener()
        new_conn.set_listener('', listener)
        try:
            new_conn.start()
            new_conn.connect()
            self.assertTrue(listener.await())
            self.assertEquals("Supported versions are 1.0,1.1\n",
                              listener.errors[0]['message'])
        finally:
            if new_conn.is_connected():
                new_conn.disconnect()

    def test_disconnect(self):
        ''' Run DISCONNECT command '''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

