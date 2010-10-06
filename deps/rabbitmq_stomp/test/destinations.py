import unittest
import stomp
import base

class TestExchange(base.BaseTest):

        
    def test_amq_direct(self):
        self.__test_exchange_send_rec("amq.direct", "route")

    def test_amq_topic(self):
        self.__test_exchange_send_rec("amq.topic", "route")

    def test_amq_fanout(self):
        self.__test_exchange_send_rec("amq.fanout", "route")

    def test_amq_fanout_no_route(self):
        self.__test_exchange_send_rec("amq.fanout")

    def test_invalid_exchange(self):
        self.listener.reset()
        self.conn.subscribe(destination="/exchange/does.not.exist")
        self.listener.await()
        self.assertEquals(1, len(self.listener.errors))
        err = self.listener.errors[0]
        self.assertEquals("not_found", err['headers']['message'])
        self.assertEquals("no exchange 'does.not.exist' in vhost '/'\n", err['message'])

    def __test_exchange_send_rec(self, exchange, route = None):
        dest = "/exchange/" + exchange
        if route != None:
            dest += "/" + route

        self.simple_test_send_rec(dest)

class TestQueue(base.BaseTest):

    def test_send_receive(self):
        d = '/queue/test'
        self.simple_test_send_rec(d)

    def test_send_receive_in_other_conn(self):
        d = '/queue/test2'

        # send
        self.conn.send("hello", destination=d)

        # now receive
        conn2 = self.createConnection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)

            conn2.subscribe(destination=d)
            self.assertTrue(listener2.await(10), "no receive")
        finally:
            conn2.stop()

    def test_send_receive_in_other_conn_with_disconnect(self):
        d = '/queue/test3'

        # send
        self.conn.send("hello thar", destination=d)
        self.conn.stop()

        # now receive
        conn2 = self.createConnection()
        try:
            listener2 = base.WaitableListener()
            conn2.set_listener('', listener2)

            conn2.subscribe(destination=d)
            self.assertTrue(listener2.await(5), "no receive")
        finally:
            conn2.stop()
        
