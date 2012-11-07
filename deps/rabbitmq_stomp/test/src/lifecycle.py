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

    def test_connect_version_1_1(self):
        ''' Test CONNECT with version 1.1'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.1")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()
            self.assertFalse(new_conn.is_connected())

    def test_heartbeat_disconnects_client(self):
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

    def test_unsupported_version(self):
        ''' Test unsupported version on CONNECT command'''
        self.bad_connect(stomp.Connection(user="guest",
                                          passcode="guest",
                                          version="100.1"),
                         "Supported versions are 1.0,1.1,1.2\n")

    def test_bad_username(self):
        ''' Test bad username'''
        self.bad_connect(stomp.Connection(user="gust",
                                          passcode="guest"),
                         "Access refused for user 'gust'\n")

    def test_bad_password(self):
        ''' Test bad password'''
        self.bad_connect(stomp.Connection(user="guest",
                                          passcode="gust"),
                         "Access refused for user 'guest'\n")

    def test_bad_vhost(self):
        ''' Test bad virtual host'''
        self.bad_connect(stomp.Connection(user="guest",
                                          passcode="guest",
                                          virtual_host="//"),
                         "Virtual host '//' access denied")

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

    def test_bad_header_on_send(self):
        ''' Test disallowed header on SEND '''
        self.listener.reset(1)
        self.conn.send_frame("SEND", {"destination":"a", "message-id":"1"})
        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.errors))
        errorReceived = self.listener.errors[0]
        self.assertEquals("Invalid header", errorReceived['headers']['message'])
        self.assertEquals("'message-id' is not allowed on 'SEND'.\n", errorReceived['message'])

    def test_disconnect(self):
        ''' Test DISCONNECT command'''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

    def test_disconnect_with_receipt(self):
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
