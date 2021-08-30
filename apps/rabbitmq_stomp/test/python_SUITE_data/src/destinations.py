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

class TestExchange(base.BaseTest):


    def test_amq_direct(self):
        ''' Test basic send/receive for /exchange/amq.direct '''
        self.__test_exchange_send_rec("amq.direct", "route")

    def test_amq_topic(self):
        ''' Test basic send/receive for /exchange/amq.topic '''
        self.__test_exchange_send_rec("amq.topic", "route")

    def test_amq_fanout(self):
        ''' Test basic send/receive for /exchange/amq.fanout '''
        self.__test_exchange_send_rec("amq.fanout", "route")

    def test_amq_fanout_no_route(self):
        ''' Test basic send/receive, /exchange/amq.direct, no routing key'''
        self.__test_exchange_send_rec("amq.fanout")

    def test_invalid_exchange(self):
        ''' Test invalid exchange error '''
        self.listener.reset(1)
        self.subscribe_dest(self.conn, "/exchange/does.not.exist", None,
                            ack="auto")
        self.assertListener("Expecting an error", numErrs=1)
        err = self.listener.errors[0]
        self.assertEqual("not_found", err['headers']['message'])
        self.assertRegex(err['message'], r'^NOT_FOUND')
        time.sleep(1)
        self.assertFalse(self.conn.is_connected())

    def __test_exchange_send_rec(self, exchange, route = None):
        if exchange != "amq.topic":
            dest = "/exchange/" + exchange
        else:
            dest = "/topic"
        if route != None:
            dest += "/" + route

        self.simple_test_send_rec(dest)

class TestQueue(base.BaseTest):

    def test_send_receive(self):
        ''' Test basic send/receive for /queue '''
        destination = '/queue/test'
        self.simple_test_send_rec(destination)

    def test_send_recv_header(self):
        ''' Test sending a custom header and receiving it back '''
        dest = '/queue/custom-header'
        hdrs = {'x-custom-header-1': 'value1',
                'x-custom-header-2': 'value2',
                'custom-header-3': 'value3'}
        self.listener.reset(1)
        recv_hdrs = self.simple_test_send_rec(dest, headers=hdrs)
        self.assertEqual('value1', recv_hdrs['x-custom-header-1'])
        self.assertEqual('value2', recv_hdrs['x-custom-header-2'])
        self.assertEqual('value3', recv_hdrs['custom-header-3'])


    def test_send_receive_in_other_conn(self):
        ''' Test send in one connection, receive in another '''
        destination = '/queue/test2'

        # send
        self.conn.send(destination, "hello")

        # now receive
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)

            self.subscribe_dest(conn2, destination, None, ack="auto")
            self.assertTrue(listener2.wait_for_complete_countdown(), "no receive")
        finally:
            conn2.disconnect()

    def test_send_receive_in_other_conn_with_disconnect(self):
        ''' Test send, disconnect, receive '''
        destination = '/queue/test3'

        # send
        self.conn.send(destination, "hello thar", receipt="foo")
        self.listener.wait_for_complete_countdown(3)
        self.conn.disconnect()

        # now receive
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)

            self.subscribe_dest(conn2, destination, None, ack="auto")
            self.assertTrue(listener2.wait_for_complete_countdown(), "no receive")
        finally:
            conn2.disconnect()


    def test_multi_subscribers(self):
        ''' Test multiple subscribers against a single /queue destination '''
        destination = '/queue/test-multi'

        ## set up two subscribers
        conn1, listener1 = self.create_subscriber_connection(destination)
        conn2, listener2 = self.create_subscriber_connection(destination)

        try:
            ## now send
            self.conn.send(destination, "test1")
            self.conn.send(destination, "test2")

            ## expect both consumers to get a message?
            self.assertTrue(listener1.wait(2))
            self.assertEqual(1, len(listener1.messages),
                              "unexpected message count")
            self.assertTrue(listener2.wait(2))
            self.assertEqual(1, len(listener2.messages),
                              "unexpected message count")
        finally:
            conn1.disconnect()
            conn2.disconnect()

    def test_send_with_receipt(self):
        destination = '/queue/test-receipt'
        def noop(): pass
        self.__test_send_receipt(destination, noop, noop)

    def test_send_with_receipt_tx(self):
        destination = '/queue/test-receipt-tx'
        tx = 'receipt.tx'

        def before():
            self.conn.begin(transaction=tx)

        def after():
            self.assertFalse(self.listener.wait(1))
            self.conn.commit(transaction=tx)

        self.__test_send_receipt(destination, before, after, {'transaction': tx})

    def test_interleaved_receipt_no_receipt(self):
        ''' Test i-leaved receipt/no receipt, no-r bracketed by rs '''

        destination = '/queue/ir'

        self.listener.reset(5)

        self.subscribe_dest(self.conn, destination, None, ack="auto")
        self.conn.send(destination, 'first', receipt='a')
        self.conn.send(destination, 'second')
        self.conn.send(destination, 'third', receipt='b')

        self.assertListener("Missing messages/receipts", numMsgs=3, numRcts=2, timeout=3)

        self.assertEqual(set(['a','b']), self.__gather_receipts())

    def test_interleaved_receipt_no_receipt_tx(self):
        ''' Test i-leaved receipt/no receipt, no-r bracketed by r+xactions '''

        destination = '/queue/ir'
        tx = 'tx.ir'

        # three messages and two receipts
        self.listener.reset(5)

        self.subscribe_dest(self.conn, destination, None, ack="auto")
        self.conn.begin(transaction=tx)

        self.conn.send(destination, 'first', receipt='a', transaction=tx)
        self.conn.send(destination, 'second', transaction=tx)
        self.conn.send(destination, 'third', receipt='b', transaction=tx)
        self.conn.commit(transaction=tx)

        self.assertListener("Missing messages/receipts", numMsgs=3, numRcts=2, timeout=40)

        expected = set(['a', 'b'])
        missing = expected.difference(self.__gather_receipts())

        self.assertEqual(set(), missing, "Missing receipts: " + str(missing))

    def test_interleaved_receipt_no_receipt_inverse(self):
        ''' Test i-leaved receipt/no receipt, r bracketed by no-rs '''

        destination = '/queue/ir'

        self.listener.reset(4)

        self.subscribe_dest(self.conn, destination, None, ack="auto")
        self.conn.send(destination, 'first')
        self.conn.send(destination, 'second', receipt='a')
        self.conn.send(destination, 'third')

        self.assertListener("Missing messages/receipt", numMsgs=3, numRcts=1, timeout=3)

        self.assertEqual(set(['a']), self.__gather_receipts())

    def __test_send_receipt(self, destination, before, after, headers = {}):
        count = 50
        self.listener.reset(count)

        before()
        expected_receipts = set()

        for x in range(0, count):
            receipt = "test" + str(x)
            expected_receipts.add(receipt)
            self.conn.send(destination, "test receipt",
                           receipt=receipt, headers=headers)
        after()

        self.assertTrue(self.listener.wait_for_complete_countdown())

        missing_receipts = expected_receipts.difference(
                    self.__gather_receipts())

        self.assertEqual(set(), missing_receipts,
                          "missing receipts: " + str(missing_receipts))

    def __gather_receipts(self):
        result = set()
        for r in self.listener.receipts:
            result.add(r['headers']['receipt-id'])
        return result

class TestTopic(base.BaseTest):

      def test_send_receive(self):
        ''' Test basic send/receive for /topic '''
        destination = '/topic/test'
        self.simple_test_send_rec(destination)

      def test_send_multiple(self):
          ''' Test /topic with multiple consumers '''
          destination = '/topic/multiple'

          ## set up two subscribers
          conn1, listener1 = self.create_subscriber_connection(destination)
          conn2, listener2 = self.create_subscriber_connection(destination)

          try:
              ## listeners are expecting 2 messages
              listener1.reset(2)
              listener2.reset(2)

              ## now send
              self.conn.send(destination, "test1")
              self.conn.send(destination, "test2")

              ## expect both consumers to get both messages
              self.assertTrue(listener1.wait_for_complete_countdown())
              self.assertEqual(2, len(listener1.messages),
                                "unexpected message count")
              self.assertTrue(listener2.wait_for_complete_countdown())
              self.assertEqual(2, len(listener2.messages),
                                "unexpected message count")
          finally:
              conn1.disconnect()
              conn2.disconnect()

      def test_send_multiple_with_a_large_message(self):
          ''' Test /topic with multiple consumers '''
          destination = '/topic/16mb'
          # payload size
          s = 1024 * 1024 * 16
          message = 'x' * s

          conn1, listener1 = self.create_subscriber_connection(destination)
          conn2, listener2 = self.create_subscriber_connection(destination)

          try:
              listener1.reset(2)
              listener2.reset(2)

              self.conn.send(destination, message)
              self.conn.send(destination, message)

              self.assertTrue(listener1.wait(10))
              self.assertEqual(2, len(listener1.messages),
                                "unexpected message count")
              self.assertTrue(len(listener2.messages[0]['message']) == s,
                              "unexpected message size")

              self.assertTrue(listener2.wait(10))
              self.assertEqual(2, len(listener2.messages),
                                "unexpected message count")
          finally:
              conn1.disconnect()
              conn2.disconnect()

class TestReplyQueue(base.BaseTest):

    def test_durable_known_reply_queue(self):
        '''As test_reply_queue, but with a non-temp reply queue'''

        known = '/queue/known'
        reply = '/queue/reply'

        ## Client 1 uses pre-supplied connection and listener
        ## Set up client 2
        conn1, listener1 = self.create_subscriber_connection(reply)
        conn2, listener2 = self.create_subscriber_connection(known)

        try:
            conn1.send(known, "test",
                       headers = {"reply-to": reply})

            self.assertTrue(listener2.wait_for_complete_countdown())
            self.assertEqual(1, len(listener2.messages))

            reply_to = listener2.messages[0]['headers']['reply-to']
            self.assertTrue(reply_to == reply)

            conn2.send(reply_to, "reply")
            self.assertTrue(listener1.wait_for_complete_countdown())
            self.assertEqual("reply", listener1.messages[0]['message'])
        finally:
            conn1.disconnect()
            conn2.disconnect()

class TestDurableSubscription(base.BaseTest):

    ID = 'test.subscription'

    def __subscribe(self, dest, conn=None, id=None):
        if not conn:
            conn = self.conn
        if not id:
            id = TestDurableSubscription.ID

        self.subscribe_dest(conn, dest, id, ack="auto",
                            headers = {'durable': 'true',
                                       'receipt': 1,
                                       'auto-delete': False})

    def __assert_receipt(self, listener=None, pos=None):
        if not listener:
            listener = self.listener

        self.assertTrue(listener.wait_for_complete_countdown())
        self.assertEqual(1, len(self.listener.receipts))
        if pos is not None:
            self.assertEqual(pos, self.listener.receipts[0]['msg_no'])

    def __assert_message(self, msg, listener=None, pos=None):
        if not listener:
            listener = self.listener

        self.assertTrue(listener.wait_for_complete_countdown())
        self.assertEqual(1, len(listener.messages))
        self.assertEqual(msg, listener.messages[0]['message'])
        if pos is not None:
            self.assertEqual(pos, self.listener.messages[0]['msg_no'])

    def do_test_durable_subscription(self, durability_header):
        destination = '/topic/durable'

        self.__subscribe(destination)
        self.__assert_receipt()

        # send first message without unsubscribing
        self.listener.reset(1)
        self.conn.send(destination, "first")
        self.__assert_message("first")

        # now unsubscribe (disconnect only)
        self.unsubscribe_dest(self.conn, destination, TestDurableSubscription.ID)

        # send again
        self.listener.reset(2)
        self.conn.send(destination, "second")

        # resubscribe and expect receipt
        self.__subscribe(destination)
        self.__assert_receipt(pos=1)
        # and message
        self.__assert_message("second", pos=2)

        # now unsubscribe (cancel)
        self.unsubscribe_dest(self.conn, destination, TestDurableSubscription.ID,
                              headers={durability_header: 'true'})

        # send again
        self.listener.reset(1)
        self.conn.send(destination, "third")

        # resubscribe and expect no message
        self.__subscribe(destination)
        self.assertTrue(self.listener.wait(3))
        self.assertEqual(0, len(self.listener.messages))
        self.assertEqual(1, len(self.listener.receipts))

    def test_durable_subscription(self):
        self.do_test_durable_subscription('durable')

    def test_durable_subscription_and_legacy_header(self):
        self.do_test_durable_subscription('persistent')

    def test_share_subscription(self):
        destination = '/topic/durable-shared'

        conn2 = self.create_connection()
        conn2.set_listener('', self.listener)

        try:
            self.__subscribe(destination)
            self.__assert_receipt()
            self.listener.reset(1)
            self.__subscribe(destination, conn2)
            self.__assert_receipt()

            self.listener.reset(100)

            n = 100
            # send 100 messages
            for x in range(0, n):
                self.conn.send(destination, "msg" + str(x))

            self.assertTrue(self.listener.wait_for_complete_countdown())
            self.assertEqual(n, len(self.listener.messages))
        finally:
            conn2.disconnect()

    def test_separate_ids(self):
        destination = '/topic/durable-separate'

        conn2 = self.create_connection()
        listener2 = base.WaitableListener()
        conn2.set_listener('', listener2)

        try:
            # ensure durable subscription exists for each ID
            self.__subscribe(destination)
            self.__assert_receipt()
            self.__subscribe(destination, conn2, "other.id")
            self.__assert_receipt(listener2)
            self.unsubscribe_dest(self.conn, destination, TestDurableSubscription.ID)
            self.unsubscribe_dest(conn2, destination, "other.id")

            self.listener.reset(11)
            listener2.reset(11) ## 10 messages and 1 receipt

            # send 100 messages
            for x in range(0, 10):
                self.conn.send(destination, "msg" + str(x))

            self.__subscribe(destination)
            self.__subscribe(destination, conn2, "other.id")

            for l in [self.listener, listener2]:
                self.assertTrue(l.wait_for_complete_countdown())
                self.assertTrue(len(l.messages) >= 9)
                self.assertTrue(len(l.messages) <= 10)

        finally:
            conn2.disconnect()

    def do_test_durable_subscribe_no_id_and_header(self, header):
        destination = '/topic/durable-invalid'

        self.conn.send_frame('SUBSCRIBE',
            {'destination': destination, 'ack': 'auto', header: 'true'})
        self.listener.wait(3)
        self.assertEqual(1, len(self.listener.errors))
        self.assertEqual("Missing Header", self.listener.errors[0]['headers']['message'])

    def test_durable_subscribe_no_id(self):
        self.do_test_durable_subscribe_no_id_and_header('durable')

    def test_durable_subscribe_no_id_and_legacy_header(self):
        self.do_test_durable_subscribe_no_id_and_header('persistent')


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)