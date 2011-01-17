import unittest
import stomp
import base
import time

class TestLifecycle(base.BaseTest):

    def test_unsubscribe_exchange_destination(self):
        ''' Test UNSUBSCRIBE command with exchange'''
        self.unsub_test(self.sub_and_send("/exchange/amq.fanout"))

    def test_unsubscribe_exchange_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with exchange'''
        d = "/exchange/amq.fanout"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d)
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send with RECEIPTs
        self.listener.reset()
        self.conn.unsubscribe(destination=d, receipt="unsub.receipt")
        self.assertTrue(self.listener.await(1),
                         "No RECEIPT received on UNSUBSCRIBE")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on UNSUBSCRIBE")

        self.listener.reset(2) # RECEIPT and possibly message
        self.conn.send("test", destination=d, receipt="send.receipt")
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE exchange failed, still receiving messages")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on SEND")

    def test_unsubscribe_queue_destination(self):
        ''' Test UNSUBSCRIBE command with queue'''
        self.unsub_test(self.sub_and_send("/queue/unsub01"))

    def test_unsubscribe_queue_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue'''
        d = "/queue/unsub03"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d)
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send with RECEIPTs
        self.listener.reset()
        self.conn.unsubscribe(destination=d, receipt="unsub.receipt")
        self.assertTrue(self.listener.await(1),
                         "No RECEIPT received on UNSUBSCRIBE")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on UNSUBSCRIBE")

        self.listener.reset(2) # RECEIPT and possibly message
        self.conn.send("test", destination=d, receipt="send.receipt")
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE queue failed, still receiving messages")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on SEND")

    def test_unsubscribe_exchange_id(self):
        ''' Test UNSUBSCRIBE command with exchange by id'''
        self.unsub_test(self.subid_and_send("/exchange/amq.fanout", "exchid"))

    def test_unsubscribe_exchange_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with exchange by id'''
        d = "/exchange/amq.fanout"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, id="exchid")
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send with RECEIPTs
        self.listener.reset()
        self.conn.unsubscribe(id="exchid", receipt="unsub.receipt")
        self.assertTrue(self.listener.await(1),
                         "No RECEIPT received on UNSUBSCRIBE")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on UNSUBSCRIBE")

        self.listener.reset(2) # RECEIPT and possibly message
        self.conn.send("test", destination=d, receipt="send.receipt")
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE exchange failed, still receiving messages")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on SEND")

    def test_unsubscribe_queue_id(self):
        ''' Test UNSUBSCRIBE command with queue by id'''
        self.unsub_test(self.subid_and_send("/queue/unsub02", "queid"))

    def test_unsubscribe_queue_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue by id'''
        d = "/queue/unsub04"

        # subscribe and send message
        self.listener.reset()
        self.conn.subscribe(destination=d, id="queid")
        self.conn.send("test", destination=d)
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.messages))
        self.assertEquals(0, len(self.listener.errors))

        # unsubscribe and send with RECEIPTs
        self.listener.reset()
        self.conn.unsubscribe(id="queid", receipt="unsub.receipt")
        self.assertTrue(self.listener.await(1),
                         "No RECEIPT received on UNSUBSCRIBE")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on UNSUBSCRIBE")

        self.listener.reset(2) # RECEIPT and possibly message
        self.conn.send("test", destination=d, receipt="send.receipt")
        self.assertFalse(self.listener.await(3),
                         "UNSUBSCRIBE queue by id failed, still receiving messages")
        self.assertEquals(1, len(self.listener.receipts),
                          "Expected a receipt on SEND")

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
