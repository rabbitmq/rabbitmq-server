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
        ''' Test basic send/receive for /exchange/amq.direct with no
        routing key'''
        self.__test_exchange_send_rec("amq.fanout")

    def test_invalid_exchange(self):
        ''' Test invalid exchange error '''
        self.listener.reset()
        self.conn.subscribe(destination="/exchange/does.not.exist")
        self.listener.await()
        self.assertEquals(1, len(self.listener.errors))
        err = self.listener.errors[0]
        self.assertEquals("not_found", err['headers']['message'])
        self.assertEquals("no exchange 'does.not.exist' in vhost '/'\n",
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

