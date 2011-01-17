import unittest
import stomp
import base
import time

class TestLifecycle(base.BaseTest):

    def test_unsubscribe_exchange_destination(self):
        ''' Test UNSUBSCRIBE command with exchange'''
        self.unsub_test(self.sub_and_send("/exchange/amq.fanout"))

    def test_unsubscribe_queue_destination(self):
        ''' Test UNSUBSCRIBE command with queue'''
        self.unsub_test(self.sub_and_send("/queue/unsub01"))

    def test_unsubscribe_exchange_id(self):
        ''' Test UNSUBSCRIBE command with exchange by id'''
        self.unsub_test(self.subid_and_send("/exchange/amq.fanout", "exchid"))

    def test_unsubscribe_queue_id(self):
        ''' Test UNSUBSCRIBE command with queue by id'''
        self.unsub_test(self.subid_and_send("/queue/unsub02", "queid"))

    def test_disconnect(self):
        ''' Run DISCONNECT command '''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

    def unsub_test(self, verbs):
        subverb, unsubverb = verbs
        self.assertListenerAfter(subverb,
                           numMsgs=1, errMsg="FAILED to subscribe and send")
        self.assertListenerAfter(unsubverb,
                           errMsg="Still receiving messages")

    def subid_and_send(self, dest, subid):
        def subfun():
            self.conn.subscribe(destination=dest, id=subid)
            self.conn.send("test", destination=dest)
        def unsubfun():
            self.conn.unsubscribe(id=subid)
        return subfun, unsubfun

    def sub_and_send(self, dest):
        def subfun():
            self.conn.subscribe(destination=dest)
            self.conn.send("test", destination=dest)
        def unsubfun():
            self.conn.unsubscribe(destination=dest)
        return subfun, unsubfun
