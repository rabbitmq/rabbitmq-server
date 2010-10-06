import unittest
import stomp
import base
import time

class TestAck(base.BaseTest):

    def test_ack_client(self):
        d = "/queue/ack-test"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, ack='client')
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await(3), "initial message not received")
        self.assertEquals(1, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.createConnection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)
            conn2.subscribe(destination=d, ack='client')
            self.assertTrue(listener2.await(), "message not received again")
            self.assertEquals(1, len(listener2.messages))

            # now ack
            mid = listener2.messages[0]['headers']['message-id']
            conn2.ack({'message-id':mid})
        finally:
            conn2.stop()

        # now reconnect again, shouldn't see the message
        conn3 = self.createConnection()
        try:
            listener3 = base.WaitableListener()
            conn3.set_listener('', listener3)
            conn3.subscribe(destination=d)
            self.assertFalse(listener3.await(3), "unexpected message. ACK not working?")
        finally:
            conn3.stop()
        
