import unittest
import stomp
import base
import time

class TestLifecycle(base.BaseTest):

    def xtest_unsubscribe_exchange_destination(self):
        ''' Test UNSUBSCRIBE command with exchange'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d))

    def xtest_unsubscribe_exchange_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with exchange'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d, receipt="unsub.rct"), numRcts=1)

    def xtest_unsubscribe_queue_destination(self):
        ''' Test UNSUBSCRIBE command with queue'''
        d = "/queue/unsub01"
        self.unsub_test(d, self.sub_and_send(d))

    def xtest_unsubscribe_queue_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue'''
        d = "/queue/unsub02"
        self.unsub_test(d, self.sub_and_send(d, receipt="unsub.rct"), numRcts=1)

    def xtest_unsubscribe_exchange_id(self):
        ''' Test UNSUBSCRIBE command with exchange by id'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d, subid="exchid"))

    def xtest_unsubscribe_exchange_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with exchange by id'''
        d = "/exchange/amq.fanout"
        self.unsub_test(d, self.sub_and_send(d, subid="exchid", receipt="unsub.rct"), numRcts=1)

    def xtest_unsubscribe_queue_id(self):
        ''' Test UNSUBSCRIBE command with queue by id'''
        d = "/queue/unsub03"
        self.unsub_test(d, self.sub_and_send(d, subid="queid"))

    def xtest_unsubscribe_queue_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue by id'''
        d = "/queue/unsub04"
        self.unsub_test(d, self.sub_and_send(d, subid="queid", receipt="unsub.rct"), numRcts=1)

    def xtest_connect_version_1_1(self):
        ''' Test CONNECT with version 1.1'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.1,1.0")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()

    def xtest_heartbeat_disconnects_client(self):
        ''' Test heart-beat disconnection'''
        self.conn.disconnect()
        new_conn = self.create_connection(heartbeat="1500,0")
        try:
            self.assertTrue(new_conn.is_connected())
            time.sleep(1)
            self.assertTrue(new_conn.is_connected())
            time.sleep(3)
            self.assertFalse(new_conn.is_connected())
        finally:
            if new_conn.is_connected():
                new_conn.disconnect()

    def xtest_unsupported_version(self):
        ''' Test unsupported version on CONNECT command'''
        self.bad_connect(stomp.Connection(user="guest",
                                          passcode="guest",
                                          version="100.1"),
                         "Supported versions are 1.0,1.1\n")

    def xtest_bad_username(self):
        ''' Test bad username'''
        self.bad_connect(stomp.Connection(user="gust",
                                          passcode="guest"),
                         "Authentication failure\n")

    def xtest_bad_password(self):
        ''' Test bad password'''
        self.bad_connect(stomp.Connection(user="guest",
                                          passcode="gust"),
                         "Authentication failure\n")

    def xtest_bad_vhost(self):
        ''' Test bad virtual host'''
        self.bad_connect(stomp.Connection(user="guest",
                                          passcode="guest",
                                          virtual_host="//"),
                         "Authentication failure\n")

    def xtest_default_user(self):
        ''' Test default user connection '''
        self.conn.disconnect()
        new_conn = stomp.Connection(user="", passcode="")
        new_conn.start()
        new_conn.connect()
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()

    def test_implicit_connect(self):
        self.conn.disconnect()
        listener = base.WaitableListener()
        new_conn = stomp.Connection(user="", passcode="")
        new_conn.set_listener('', listener)

        new_conn.start() # not going to issue connect
        new_conn.subscribe(destination="/topic/implicit", receipt='implicit')

        try:
            self.assertTrue(listener.await(5))
            self.assertEquals(1, len(listener.receipts),
                              'Missing receipt. Likely not connected')
            self.assertEquals('implicit', listener.receipts[0]['headers']['receipt-id'])
        finally:
            new_conn.disconnect()

    def bad_connect(self, new_conn, expected):
        self.conn.disconnect()
        listener = base.WaitableListener()
        new_conn.set_listener('', listener)
        try:
            new_conn.start()
            new_conn.connect()
            self.assertTrue(listener.await())
            self.assertEquals(expected, listener.errors[0]['message'])
        finally:
            if new_conn.is_connected():
                new_conn.disconnect()

    def xtest_disconnect(self):
        ''' Test DISCONNECT command'''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

    def xtest_disconnect_with_receipt(self):
        ''' Test the DISCONNECT command with receipts '''
        time.sleep(3)
        self.listener.reset(1)
        self.conn.send_frame("DISCONNECT", {"receipt": "test"})
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.receipts))
        receiptReceived = self.listener.receipts[0]['headers']['receipt-id']
        self.assertEquals("test", receiptReceived
                         , "Wrong receipt received: '" + receiptReceived + "'")

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
