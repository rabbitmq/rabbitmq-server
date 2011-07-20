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
        self.listener.reset()
        self.conn.subscribe(destination="/exchange/does.not.exist")
        self.listener.await()
        self.assertEquals(1, len(self.listener.errors))
        err = self.listener.errors[0]
        self.assertEquals("not_found", err['headers']['message'])
        self.assertEquals(
            "NOT_FOUND - no exchange 'does.not.exist' in vhost '/'\n",
            err['message'])
        time.sleep(1)
        self.assertFalse(self.conn.is_connected())

    def __test_exchange_send_rec(self, exchange, route = None):
        dest = "/exchange/" + exchange
        if route != None:
            dest += "/" + route

        self.simple_test_send_rec(dest)

class TestQueue(base.BaseTest):

    def test_send_receive(self):
        ''' Test basic send/receive for /queue '''
        d = '/queue/test'
        self.simple_test_send_rec(d)

    def test_send_receive_in_other_conn(self):
        ''' Test send in one connection, receive in another '''
        d = '/queue/test2'

        # send
        self.conn.send("hello", destination=d)

        # now receive
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)

            conn2.subscribe(destination=d)
            self.assertTrue(listener2.await(10), "no receive")
        finally:
            conn2.stop()

    def test_send_receive_in_other_conn_with_disconnect(self):
        ''' Test send, disconnect, receive '''
        d = '/queue/test3'

        # send
        self.conn.send("hello thar", destination=d, receipt="foo")
        self.listener.await(3)
        self.conn.stop()

        # now receive
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)

            conn2.subscribe(destination=d)
            self.assertTrue(listener2.await(10), "no receive")
        finally:
            conn2.stop()


    def test_multi_subscribers(self):
        ''' Test multiple subscribers against a single /queue destination '''
        d = '/queue/test-multi'

        ## set up two subscribers
        conn1, listener1 = self.create_subscriber_connection(d)
        conn2, listener2 = self.create_subscriber_connection(d)

        try:
            ## now send
            self.conn.send("test1", destination=d)
            self.conn.send("test2", destination=d)

            ## expect both consumers to get a message?
            self.assertTrue(listener1.await(2))
            self.assertEquals(1, len(listener1.messages),
                              "unexpected message count")
            self.assertTrue(listener2.await(2))
            self.assertEquals(1, len(listener2.messages),
                              "unexpected message count")
        finally:
            conn1.stop()
            conn2.stop()

    def test_send_with_receipt(self):
        d = '/queue/test-receipt'
        def noop(): pass
        self.__test_send_receipt(d, noop, noop)

    def test_send_with_receipt_tx(self):
        d = '/queue/test-receipt-tx'
        tx = 'receipt.tx'

        def before():
            self.conn.begin(transaction=tx)

        def after():
            self.assertFalse(self.listener.await(1))
            self.conn.commit(transaction=tx)

        self.__test_send_receipt(d, before, after, {'transaction': tx})

    def test_interleaved_receipt_no_receipt(self):
        ''' Test i-leaved receipt/no receipt, no-r bracketed by rs '''

        d = '/queue/ir'

        self.listener.reset(5)

        self.conn.subscribe(destination=d)
        self.conn.send('first', destination=d, receipt='a')
        self.conn.send('second', destination=d)
        self.conn.send('third', destination=d, receipt='b')

        self.assertTrue(self.listener.await(3))

        self.assertEquals(set(['a','b']), self.__gather_receipts())
        self.assertEquals(3, len(self.listener.messages))

    def test_interleaved_receipt_no_receipt_tx(self):
        ''' Test i-leaved receipt/no receipt, no-r bracketed by r+xactions '''

        d = '/queue/ir'
        tx = 'tx.ir'

        # three messages and two receipts
        self.listener.reset(5)

        self.conn.subscribe(destination=d)
        self.conn.begin(transaction=tx)

        self.conn.send('first', destination=d, receipt='a', transaction=tx)
        self.conn.send('second', destination=d, transaction=tx)
        self.conn.send('third', destination=d, receipt='b', transaction=tx)
        self.conn.commit(transaction=tx)

        self.assertTrue("Missing messages/confirms", self.listener.await(20))

        expected = set(['a', 'b'])
        missing = expected.difference(self.__gather_receipts())

        self.assertEquals(set(), missing, "Missing receipts: " + str(missing))
        self.assertEquals(3, len(self.listener.messages))

    def test_interleaved_receipt_no_receipt_inverse(self):
        ''' Test i-leaved receipt/no receipt, r bracketed by no-rs '''

        d = '/queue/ir'

        self.listener.reset(4)

        self.conn.subscribe(destination=d)
        self.conn.send('first', destination=d)
        self.conn.send('second', destination=d, receipt='a')
        self.conn.send('third', destination=d)

        self.assertTrue(self.listener.await(3))

        self.assertEquals(set(['a']), self.__gather_receipts())
        self.assertEquals(3, len(self.listener.messages))

    def __test_send_receipt(self, destination, before, after, headers = {}):
        count = 50
        self.listener.reset(count)

        before()
        expected_receipts = set()

        for x in range(0, count):
            receipt = "test" + str(x)
            expected_receipts.add(receipt)
            self.conn.send("test receipt", destination=destination,
                           receipt=receipt, headers=headers)
        after()

        self.assertTrue(self.listener.await(5))

        missing_receipts = expected_receipts.difference(
                    self.__gather_receipts())

        self.assertEquals(set(), missing_receipts,
                          "missing receipts: " + str(missing_receipts))

    def __gather_receipts(self):
        result = set()
        for r in self.listener.receipts:
            result.add(r['headers']['receipt-id'])
        return result

class TestTopic(base.BaseTest):

      def test_send_receive(self):
        ''' Test basic send/receive for /topic '''
        d = '/topic/test'
        self.simple_test_send_rec(d)

      def test_send_multiple(self):
          ''' Test /topic with multiple consumers '''
          d = '/topic/multiple'

          ## set up two subscribers
          conn1, listener1 = self.create_subscriber_connection(d)
          conn2, listener2 = self.create_subscriber_connection(d)

          try:
              ## listeners are expecting 2 messages
              listener1.reset(2)
              listener2.reset(2)

              ## now send
              self.conn.send("test1", destination=d)
              self.conn.send("test2", destination=d)

              ## expect both consumers to get both messages
              self.assertTrue(listener1.await(5))
              self.assertEquals(2, len(listener1.messages),
                                "unexpected message count")
              self.assertTrue(listener2.await(5))
              self.assertEquals(2, len(listener2.messages),
                                "unexpected message count")
          finally:
              conn1.stop()
              conn2.stop()

class TestReplyQueue(base.BaseTest):

    def test_reply_queue(self):
        ''' Test with two separate clients. Client 1 sends
        message to a known destination with a defined reply
        queue. Client 2 receives on known destination and replies
        on the reply destination. Client 1 gets the reply message'''

        known = '/queue/known'
        reply = '/temp-queue/0'

        ## Client 1 uses pre-supplied connection and listener
        ## Set up client 2
        conn2, listener2 = self.create_subscriber_connection(known)

        try:
            self.conn.send("test", destination=known,
                           headers = {"reply-to": reply})

            self.assertTrue(listener2.await(5))
            self.assertEquals(1, len(listener2.messages))

            reply_to = listener2.messages[0]['headers']['reply-to']
            self.assertTrue(reply_to.startswith('/reply-queue/'))

            conn2.send("reply", destination=reply_to)
            self.assertTrue(self.listener.await(5))
            self.assertEquals("reply", self.listener.messages[0]['message'])
        finally:
            conn2.stop()

class TestDurableSubscription(base.BaseTest):

    ID = 'test.subscription'

    def __subscribe(self, dest, conn=None, id=None):
        if not conn:
            conn = self.conn
        if not id:
            id = TestDurableSubscription.ID

        conn.subscribe(destination=dest,
                       headers    ={'persistent': 'true',
                                    'receipt': 1,
                                    'id': id})

    def __assert_receipt(self, listener=None):
        if not listener:
            listener = self.listener

        self.assertTrue(listener.await(5))
        self.assertEquals(1, len(self.listener.receipts))

    def __assert_message(self, msg, listener=None):
        if not listener:
            listener = self.listener

        self.assertTrue(listener.await(5))
        self.assertEquals(1, len(listener.messages))
        self.assertEquals(msg, listener.messages[0]['message'])

    def test_durability(self):
        d = '/topic/durable'

        self.__subscribe(d)
        self.__assert_receipt()

        # send first message without unsubscribing
        self.listener.reset(1)
        self.conn.send("first", destination=d)
        self.__assert_message("first")

        # now unsubscribe (disconnect only)
        self.conn.unsubscribe(id=TestDurableSubscription.ID)

        # send again
        self.listener.reset(1)
        self.conn.send("second", destination=d)

        # resubscribe and expect message
        self.__subscribe(d)
        self.__assert_message("second")

        # now unsubscribe (cancel)
        self.conn.unsubscribe(id=TestDurableSubscription.ID,
                              headers={'persistent': 'true'})

        # send again
        self.listener.reset(1)
        self.conn.send("third", destination=d)

        # resubscribe and expect no message
        self.__subscribe(d)
        self.assertTrue(self.listener.await(3))
        self.assertEquals(0, len(self.listener.messages))
        self.assertEquals(1, len(self.listener.receipts))

    def test_share_subscription(self):
        d = '/topic/durable-shared'

        conn2 = self.create_connection()
        conn2.set_listener('', self.listener)

        try:
            self.__subscribe(d)
            self.__assert_receipt()
            self.listener.reset(1)
            self.__subscribe(d, conn2)
            self.__assert_receipt()

            self.listener.reset(100)

            # send 100 messages
            for x in xrange(0, 100):
                self.conn.send("msg" + str(x), destination=d)

            self.assertTrue(self.listener.await(5))
            self.assertEquals(100, len(self.listener.messages))
        finally:
            conn2.stop()

    def test_separate_ids(self):
        d = '/topic/durable-separate'

        conn2 = self.create_connection()
        listener2 = base.WaitableListener()
        conn2.set_listener('', listener2)

        try:
            # ensure durable subscription exists for each ID
            self.__subscribe(d)
            self.__assert_receipt()
            self.__subscribe(d, conn2, "other.id")
            self.__assert_receipt(listener2)
            self.conn.unsubscribe(id=TestDurableSubscription.ID)
            conn2.unsubscribe(id="other.id")

            self.listener.reset(101)
            listener2.reset(101) ## 100 messages and 1 receipt

            # send 100 messages
            for x in xrange(0, 100):
                self.conn.send("msg" + str(x), destination=d)

            self.__subscribe(d)
            self.__subscribe(d, conn2, "other.id")

            for l in [self.listener, listener2]:
                self.assertTrue(l.await(10))
                self.assertEquals(100, len(l.messages))

        finally:
            conn2.stop()

    def test_durable_subscribe_no_id(self):
        d = '/topic/durable-invalid'

        self.conn.subscribe(destination=d, headers={'persistent':'true'}),
        self.listener.await(3)
        self.assertEquals(1, len(self.listener.errors))
        self.assertEquals("Missing Header", self.listener.errors[0]['headers']['message'])


