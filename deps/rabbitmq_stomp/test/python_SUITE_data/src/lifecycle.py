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

    def test_connect_version_1_0(self):
        ''' Test CONNECT with version 1.0'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.0")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()
            self.assertFalse(new_conn.is_connected())

    def test_connect_version_1_1(self):
        ''' Test CONNECT with version 1.1'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.1")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()
            self.assertFalse(new_conn.is_connected())

    def test_connect_version_1_2(self):
        ''' Test CONNECT with version 1.2'''
        self.conn.disconnect()
        new_conn = self.create_connection(version="1.2")
        try:
            self.assertTrue(new_conn.is_connected())
        finally:
            new_conn.disconnect()
            self.assertFalse(new_conn.is_connected())

    def test_heartbeat_disconnects_client(self):
        ''' Test heart-beat disconnection'''
        self.conn.disconnect()
        new_conn = self.create_connection(version='1.1', heartbeats=(1500, 0))
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
        self.bad_connect("Supported versions are 1.0,1.1,1.2\n", version='100.1')

    def test_bad_username(self):
        ''' Test bad username'''
        self.bad_connect("Access refused for user 'gust'\n", user='gust')

    def test_bad_password(self):
        ''' Test bad password'''
        self.bad_connect("Access refused for user 'guest'\n", passcode='gust')

    def test_bad_vhost(self):
        ''' Test bad virtual host'''
        self.bad_connect("Virtual host '//' access denied", version='1.1', vhost='//')

    def bad_connect(self, expected, user='guest', passcode='guest', **kwargs):
        self.conn.disconnect()
        new_conn = self.create_connection_obj(**kwargs)
        listener = base.WaitableListener()
        new_conn.set_listener('', listener)
        try:
            new_conn.start()
            new_conn.connect(user, passcode)
            self.assertTrue(listener.wait())
            self.assertEquals(expected, listener.errors[0]['message'])
        finally:
            if new_conn.is_connected():
                new_conn.disconnect()

    def test_bad_header_on_send(self):
        ''' Test disallowed header on SEND '''
        self.listener.reset(1)
        self.conn.send_frame("SEND", {"destination":"a", "message-id":"1"})
        self.assertTrue(self.listener.wait())
        self.assertEquals(1, len(self.listener.errors))
        errorReceived = self.listener.errors[0]
        self.assertEquals("Invalid header", errorReceived['headers']['message'])
        self.assertEquals("'message-id' is not allowed on 'SEND'.\n", errorReceived['message'])

    def test_send_recv_header(self):
        ''' Test sending a custom header and receiving it back '''
        dest = '/queue/custom-header'
        hdrs = {'x-custom-header-1': 'value1',
                'x-custom-header-2': 'value2',
                'custom-header-3': 'value3'}
        self.listener.reset(1)
        recv_hdrs = self.simple_test_send_rec(dest, headers=hdrs)
        self.assertEquals('value1', recv_hdrs['x-custom-header-1'])
        self.assertEquals('value2', recv_hdrs['x-custom-header-2'])
        self.assertEquals('value3', recv_hdrs['custom-header-3'])

    def test_disconnect(self):
        ''' Test DISCONNECT command'''
        self.conn.disconnect()
        self.assertFalse(self.conn.is_connected())

    def test_disconnect_with_receipt(self):
        ''' Test the DISCONNECT command with receipts '''
        time.sleep(3)
        self.listener.reset(1)
        self.conn.send_frame("DISCONNECT", {"receipt": "test"})
        self.assertTrue(self.listener.wait())
        self.assertEquals(1, len(self.listener.receipts))
        receiptReceived = self.listener.receipts[0]['headers']['receipt-id']
        self.assertEquals("test", receiptReceived
                         , "Wrong receipt received: '" + receiptReceived + "'")

    def unsub_test(self, dest, verbs, numRcts=0):
        def afterfun():
            self.conn.send(dest, "after-test")
        subverb, unsubverb = verbs
        self.assertListenerAfter(subverb, numMsgs=1,
                           errMsg="FAILED to subscribe and send")
        self.assertListenerAfter(unsubverb, numRcts=numRcts,
                           errMsg="Incorrect responses from UNSUBSCRIBE")
        self.assertListenerAfter(afterfun,
                           errMsg="Still receiving messages")

    def sub_and_send(self, dest, subid=None, receipt=None):
        def subfun():
            self.subscribe_dest(self.conn, dest, subid)
            self.conn.send(dest, "test")
        def unsubfun():
            headers = {}
            if receipt != None:
                headers['receipt'] = receipt
            self.unsubscribe_dest(self.conn, dest, subid, **headers)
        return subfun, unsubfun
