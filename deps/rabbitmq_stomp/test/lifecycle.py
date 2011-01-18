import unittest
import stomp
import base
import time

class TestLifecycle(base.BaseTest):

    def test_unsubscribe_exchange_destination(self):
        ''' Test UNSUBSCRIBE command with exchange'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d))

    def test_unsubscribe_exchange_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with exchange'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d, receipt="unsub.rct"), numRcts=1)

    def test_unsubscribe_queue_destination(self):
        ''' Test UNSUBSCRIBE command with queue'''
        d = "/queue/unsub01"
        self.unsub_test(d, self.sub_and_send(d))

    def test_unsubscribe_queue_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue'''
        d = "/queue/unsub02"
        self.unsub_test(d, self.sub_and_send(d, receipt="unsub.rct"), numRcts=1)

    def test_unsubscribe_exchange_id(self):
        ''' Test UNSUBSCRIBE command with exchange by id'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d, subid="exchid"))

    def test_unsubscribe_exchange_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with exchange by id'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d, subid="exchid", receipt="unsub.rct"), numRcts=1)

    def test_unsubscribe_queue_id(self):
        ''' Test UNSUBSCRIBE command with queue by id'''
        d = "/queue/unsub03"
        self.unsub_test(d, self.sub_and_send(d, subid="queid"))

    def test_unsubscribe_queue_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue by id'''
        d = "/queue/unsub04"
        self.unsub_test(d, self.sub_and_send(d, subid="queid", receipt="unsub.rct"), numRcts=1)

    def test_disconnect(self):
        ''' Run DISCONNECT command '''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

    def unsub_test(self, dest, verbs, numRcts=0):
        def afterfun():
            self.conn.send("after-test", destination=dest)
        subverb, unsubverb = verbs
        self.assertListenerAfter(subverb, numMsgs=1,
                           errMsg="FAILED to subscribe and send")
        self.assertListenerAfter(unsubverb, numRcts=numRcts, 
                           errMsg="Incorrect responses from UNSUBSCRIBE")
        self.assertListenerAfter(afterfun,
                           errMsg="Still receiving messages")

    def sub_and_send(self, dest, subid="", receipt=""):
        def subfun():
            if subid=="":
                self.conn.subscribe(destination=dest)
            else:
                self.conn.subscribe(destination=dest, id=subid)
            self.conn.send("test", destination=dest)
        def unsubfun():
            if subid=="" and receipt=="":
                self.conn.unsubscribe(destination=dest)
            elif receipt=="":
                self.conn.unsubscribe(id=subid)
            elif subid=="":
                self.conn.unsubscribe(destination=dest, receipt=receipt)
            else:
                self.conn.unsubscribe(id=subid, receipt=receipt)
        return subfun, unsubfun
