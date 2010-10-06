import unittest
import stomp
import base
import time

class TestAck(base.BaseTest):

    def test_ack_client(self):
        d = "/exchange/amq.direct/test"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, ack='client')
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await(3), "initial message not received")
        self.assertEquals(1, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        self.listener.reset()
        conn2 = self.createConnection()
        conn2.subscribe(destination=d, ack='client')
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        
