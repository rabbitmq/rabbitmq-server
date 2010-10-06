import unittest
import stomp
import sys
import threading


class BaseTest(unittest.TestCase):

   def createConnection(self):
       conn = stomp.Connection(user="guest", passcode="guest")
       conn.start()
       conn.connect()
       return conn
   
   def setUp(self):
        self.conn = self.createConnection()
        self.listener = WaitableListener()
        self.conn.set_listener('', self.listener)

   def tearDown(self):
        if self.conn.is_connected():
            self.conn.stop()

   def simple_test_send_rec(self, dest, route = None):
        self.listener.reset()

        self.conn.subscribe(destination=dest)
        self.conn.send("foo", destination=dest)

        self.assertTrue(self.listener.await(), "Timeout, no message received")

        # assert no errors
        if len(self.listener.errors) > 0:
            self.fail(self.listener.errors[0]['message'])
        
        # check header content
        msg = self.listener.messages[0]
        self.assertEquals("foo", msg['message'])
        self.assertEquals(dest, msg['headers']['destination'])


class WaitableListener(object):

    def __init__(self):
        self.messages = []
        self.errors = []
        self.receipts = []
        self.event = threading.Event()


    def on_receipt(self, headers, message):
        self.receipt.append({'message' : message, 'headers' : headers})
        self.event.set()
        
    def on_error(self, headers, message):
        self.errors.append({'message' : message, 'headers' : headers})
        self.event.set()

    def on_message(self, headers, message):
        self.messages.append({'message' : message, 'headers' : headers})
        self.event.set()

    def reset(self):
        self.messages = []
        self.errors = []
        self.event.clear()

    def await(self, timeout=10):
        self.event.wait(timeout)
        return self.event.is_set()
        
