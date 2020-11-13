import unittest
import stomp
import base
import time

class TestRedelivered(base.BaseTest):

    def test_redelivered(self):
        destination = "/queue/redelivered-test"

        # subscribe and send message
        self.subscribe_dest(self.conn, destination, None, ack='client')
        self.conn.send(destination, "test1")
        message_receive_timeout = 30
        self.assertTrue(self.listener.wait(message_receive_timeout), "Test message not received within {0} seconds".format(message_receive_timeout))
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals('false', self.listener.messages[0]['headers']['redelivered'])

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            listener2.reset(1)
            conn2.set_listener('', listener2)
            self.subscribe_dest(conn2, destination, None, ack='client')
            self.assertTrue(listener2.wait(), "message not received again")
            self.assertEquals(1, len(listener2.messages))
            self.assertEquals('true', listener2.messages[0]['headers']['redelivered'])
        finally:
            conn2.disconnect()
