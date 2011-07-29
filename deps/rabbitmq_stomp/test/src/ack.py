import unittest
import stomp
import base
import time

class TestAck(base.BaseTest):

    def test_ack_client(self):
        d = "/queue/ack-test"

        # subscribe and send message
        self.listener.reset(2) ## expecting 2 messages
        self.conn.subscribe(destination=d, ack='client',
                            headers={'prefetch-count': '10'})
        self.conn.send("test1", destination=d)
        self.conn.send("test2", destination=d)
        self.assertTrue(self.listener.await(4), "initial message not received")
        self.assertEquals(2, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            listener2.reset(2)
            conn2.set_listener('', listener2)
            conn2.subscribe(destination=d, ack='client',
                            headers={'prefetch-count': '10'})
            self.assertTrue(listener2.await(), "message not received again")
            self.assertEquals(2, len(listener2.messages))

            # now ack only the last message - expecting cumulative behaviour
            mid = listener2.messages[1]['headers']['message-id']
            conn2.ack({'message-id':mid})
        finally:
            conn2.stop()

        # now reconnect again, shouldn't see the message
        conn3 = self.create_connection()
        try:
            listener3 = base.WaitableListener()
            conn3.set_listener('', listener3)
            conn3.subscribe(destination=d)
            self.assertFalse(listener3.await(3),
                             "unexpected message. ACK not working?")
        finally:
            conn3.stop()

    def test_ack_client_individual(self):
        d = "/queue/ack-test-individual"

        # subscribe and send message
        self.listener.reset(2) ## expecting 2 messages
        self.conn.subscribe(destination=d, ack='client-individual',
                            headers={'prefetch-count': '10'})
        self.conn.send("test1", destination=d)
        self.conn.send("test2", destination=d)
        self.assertTrue(self.listener.await(4), "initial message not received")
        self.assertEquals(2, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            listener2.reset(2)
            conn2.set_listener('', listener2)
            conn2.subscribe(destination=d, ack='client-individual',
                            headers={'prefetch-count': '10'})
            self.assertTrue(listener2.await(), "message not received again")
            self.assertEquals(2, len(listener2.messages))

            # now ack only the last message - expecting individual behaviour
            mid = listener2.messages[1]['headers']['message-id']
            conn2.ack({'message-id':mid})
        finally:
            conn2.stop()

        # now reconnect again, shouldn't see the message
        conn3 = self.create_connection()
        try:
            listener3 = base.WaitableListener()
            conn3.set_listener('', listener3)
            conn3.subscribe(destination=d)
            self.assertTrue(listener3.await(20),
                             "Expected to see a message. ACK not working?")
            self.assertEquals("test1", listener3.messages[0]['message'])
        finally:
            conn3.stop()

    def test_ack_client_tx(self):
        d = "/queue/ack-test-tx"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, ack='client')
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await(3), "initial message not received")
        self.assertEquals(1, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            tx = "abc"
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)
            conn2.begin(transaction=tx)
            conn2.subscribe(destination=d, ack='client')
            self.assertTrue(listener2.await(), "message not received again")
            self.assertEquals(1, len(listener2.messages))

            # now ack
            mid = listener2.messages[0]['headers']['message-id']
            conn2.ack({'message-id':mid, 'transaction':tx})

            #now commit
            conn2.commit(transaction=tx)
        finally:
            conn2.stop()

        # now reconnect again, shouldn't see the message
        conn3 = self.create_connection()
        try:
            listener3 = base.WaitableListener()
            conn3.set_listener('', listener3)
            conn3.subscribe(destination=d)
            self.assertFalse(listener3.await(3),
                             "unexpected message. TX ACK not working?")
        finally:
            conn3.stop()

    def test_topic_prefetch(self):
        d = "/topic/prefetch-test"

        # subscribe and send message
        self.listener.reset(6) ## expecting 6 messages
        self.conn.subscribe(destination=d, ack='client',
                            headers={'prefetch-count': '5'})

        for x in range(10):
            self.conn.send("test" + str(x), destination=d)

        self.assertFalse(self.listener.await(5),
                         "Should not have been able to see 6 messages")
        self.assertEquals(5, len(self.listener.messages))

    def test_nack(self):
        d = "/queue/nack-test"

        #subscribe and send
        self.conn.subscribe(destination=d, ack='client-individual')
        self.conn.send("nack-test", destination=d)

        self.assertTrue(self.listener.await(), "Not received message")
        message_id = self.listener.messages[0]['headers']['message-id']
        self.listener.reset()

        self.conn.send_frame("NACK", {"message-id" : message_id})
        self.assertTrue(self.listener.await(), "Not received message again")
        message_id = self.listener.messages[0]['headers']['message-id']
        self.conn.ack({'message-id' : message_id})

    def test_nack_multi(self):
        d = "/queue/nack-multi"

        self.listener.reset(2)

        #subscribe and send
        self.conn.subscribe(destination=d, ack='client',
                            headers = {'prefetch-count' : '10'})
        self.conn.send("nack-test1", destination=d)
        self.conn.send("nack-test2", destination=d)

        self.assertTrue(self.listener.await(), "Not received messages")
        message_id = self.listener.messages[1]['headers']['message-id']
        self.listener.reset(2)

        self.conn.send_frame("NACK", {"message-id" : message_id})
        self.assertTrue(self.listener.await(), "Not received message again")
        message_id = self.listener.messages[1]['headers']['message-id']
        self.conn.ack({'message-id' : message_id})
