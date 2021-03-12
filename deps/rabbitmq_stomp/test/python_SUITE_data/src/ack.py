## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import unittest
import stomp
import base
import time
import os

class TestAck(base.BaseTest):

    def test_ack_client(self):
        destination = "/queue/ack-test"

        # subscribe and send message
        self.listener.reset(2) ## expecting 2 messages
        self.subscribe_dest(self.conn, destination, None,
                            ack='client',
                            headers={'prefetch-count': '10'})
        self.conn.send(destination, "test1")
        self.conn.send(destination, "test2")
        self.assertTrue(self.listener.wait(4), "initial message not received")
        self.assertEqual(2, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            listener2.reset(2)
            conn2.set_listener('', listener2)
            self.subscribe_dest(conn2, destination, None,
                                ack='client',
                                headers={'prefetch-count': '10'})
            self.assertTrue(listener2.wait(), "message not received again")
            self.assertEqual(2, len(listener2.messages))

            # now ack only the last message - expecting cumulative behaviour
            mid = listener2.messages[1]['headers'][self.ack_id_source_header]
            self.ack_message(conn2, mid, None)
        finally:
            conn2.disconnect()

        # now reconnect again, shouldn't see the message
        conn3 = self.create_connection()
        try:
            listener3 = base.WaitableListener()
            conn3.set_listener('', listener3)
            self.subscribe_dest(conn3, destination, None)
            self.assertFalse(listener3.wait(3),
                             "unexpected message. ACK not working?")
        finally:
            conn3.disconnect()

    def test_ack_client_individual(self):
        destination = "/queue/ack-test-individual"

        # subscribe and send message
        self.listener.reset(2) ## expecting 2 messages
        self.subscribe_dest(self.conn, destination, None,
                            ack='client-individual',
                            headers={'prefetch-count': '10'})
        self.conn.send(destination, "test1")
        self.conn.send(destination, "test2")
        self.assertTrue(self.listener.wait(4), "Both initial messages not received")
        self.assertEqual(2, len(self.listener.messages))

        # disconnect without acks
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            listener2.reset(2) ## expect 2 messages
            conn2.set_listener('', listener2)
            self.subscribe_dest(conn2, destination, None,
                                ack='client-individual',
                                headers={'prefetch-count': '10'})
            self.assertTrue(listener2.wait(2.5), "Did not receive 2 messages")
            self.assertEqual(2, len(listener2.messages), "Not exactly 2 messages received")

            # now ack only the 'test2' message - expecting individual behaviour
            nummsgs = len(listener2.messages)
            mid = None
            for ind in range(nummsgs):
                if listener2.messages[ind]['message']=="test2":
                    mid = listener2.messages[ind]['headers'][self.ack_id_source_header]
                    self.assertEqual(1, ind, 'Expecting test2 to be second message')
                    break
            self.assertTrue(mid, "Did not find test2 message id.")
            self.ack_message(conn2, mid, None)
        finally:
            conn2.disconnect()

        # now reconnect again, shouldn't see the message
        conn3 = self.create_connection()
        try:
            listener3 = base.WaitableListener()
            listener3.reset(2) ## expecting a single message, but wait for two
            conn3.set_listener('', listener3)
            self.subscribe_dest(conn3, destination, None)
            self.assertFalse(listener3.wait(2.5),
                             "Expected to see only one message. ACK not working?")
            self.assertEqual(1, len(listener3.messages), "Expecting exactly one message")
            self.assertEqual("test1", listener3.messages[0]['message'], "Unexpected message remains")
        finally:
            conn3.disconnect()

    def test_ack_client_tx(self):
        destination = "/queue/ack-test-tx"

        # subscribe and send message
        self.listener.reset()
        self.subscribe_dest(self.conn, destination, None, ack='client')
        self.conn.send(destination, "test")
        self.assertTrue(self.listener.wait(3), "initial message not received")
        self.assertEqual(1, len(self.listener.messages))

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            tx = "abc"
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)
            conn2.begin(transaction=tx)
            self.subscribe_dest(conn2, destination, None, ack='client')
            self.assertTrue(listener2.wait(), "message not received again")
            self.assertEqual(1, len(listener2.messages))

            # now ack
            mid = listener2.messages[0]['headers'][self.ack_id_source_header]
            self.ack_message(conn2, mid, None, transaction=tx)

            #now commit
            conn2.commit(transaction=tx)
        finally:
            conn2.disconnect()

        # now reconnect again, shouldn't see the message
        conn3 = self.create_connection()
        try:
            listener3 = base.WaitableListener()
            conn3.set_listener('', listener3)
            self.subscribe_dest(conn3, destination, None)
            self.assertFalse(listener3.wait(3),
                             "unexpected message. TX ACK not working?")
        finally:
            conn3.disconnect()

    def test_topic_prefetch(self):
        destination = "/topic/prefetch-test"

        # subscribe and send message
        self.listener.reset(6) ## expect 6 messages
        self.subscribe_dest(self.conn, destination, None,
                            ack='client',
                            headers={'prefetch-count': '5'})

        for x in range(10):
            self.conn.send(destination, "test" + str(x))

        self.assertFalse(self.listener.wait(3),
                         "Should not have been able to see 6 messages")
        self.assertEqual(5, len(self.listener.messages))

    def test_nack(self):
        destination = "/queue/nack-test"

        #subscribe and send
        self.subscribe_dest(self.conn, destination, None,
                            ack='client-individual')
        self.conn.send(destination, "nack-test")

        self.assertTrue(self.listener.wait(), "Not received message")
        message_id = self.listener.messages[0]['headers'][self.ack_id_source_header]
        self.listener.reset()

        self.nack_message(self.conn, message_id, None)
        self.assertTrue(self.listener.wait(), "Not received message after NACK")
        message_id = self.listener.messages[0]['headers'][self.ack_id_source_header]
        self.ack_message(self.conn, message_id, None)

    def test_nack_multi(self):
        destination = "/queue/nack-multi"

        self.listener.reset(2)

        #subscribe and send
        self.subscribe_dest(self.conn, destination, None,
                            ack='client',
                            headers = {'prefetch-count' : '10'})
        self.conn.send(destination, "nack-test1")
        self.conn.send(destination, "nack-test2")

        self.assertTrue(self.listener.wait(), "Not received messages")
        message_id = self.listener.messages[1]['headers'][self.ack_id_source_header]
        self.listener.reset(2)

        self.nack_message(self.conn, message_id, None)
        self.assertTrue(self.listener.wait(), "Not received message again")
        message_id = self.listener.messages[1]['headers'][self.ack_id_source_header]
        self.ack_message(self.conn, message_id, None)

    def test_nack_without_requeueing(self):
        destination = "/queue/nack-test-no-requeue"

        self.subscribe_dest(self.conn, destination, None,
                            ack='client-individual')
        self.conn.send(destination, "nack-test")

        self.assertTrue(self.listener.wait(), "Not received message")
        message_id = self.listener.messages[0]['headers'][self.ack_id_source_header]
        self.listener.reset()

        self.conn.send_frame("NACK", {self.ack_id_header: message_id, "requeue": False})
        self.assertFalse(self.listener.wait(4), "Received message after NACK with requeue = False")

class TestAck11(TestAck):

   def create_connection_obj(self, version='1.1', vhost='/', heartbeats=(0, 0)):
       conn = stomp.StompConnection11(host_and_ports=[('localhost', int(os.environ["STOMP_PORT"]))],
                                      vhost=vhost,
                                      heartbeats=heartbeats)
       self.ack_id_source_header = 'message-id'
       self.ack_id_header = 'message-id'
       return conn

   def test_version(self):
       self.assertEqual('1.1', self.conn.version)

class TestAck12(TestAck):

   def create_connection_obj(self, version='1.2', vhost='/', heartbeats=(0, 0)):
       conn = stomp.StompConnection12(host_and_ports=[('localhost', int(os.environ["STOMP_PORT"]))],
                                      vhost=vhost,
                                      heartbeats=heartbeats)
       self.ack_id_source_header = 'ack'
       self.ack_id_header = 'id'
       return conn

   def test_version(self):
       self.assertEqual('1.2', self.conn.version)



if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)