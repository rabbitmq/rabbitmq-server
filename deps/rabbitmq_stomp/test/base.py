import unittest
import stomp
import sys
import threading


class BaseTest(unittest.TestCase):

   def create_connection(self):
       conn = stomp.Connection(user="guest", passcode="guest")
       conn.start()
       conn.connect()
       return conn

   def create_subscriber_connection(self, dest):
       conn = self.create_connection()
       listener = WaitableListener()
       conn.set_listener('', listener)
       conn.subscribe(destination=dest, receipt="sub.receipt")
       listener.await()
       self.assertEquals(1, len(listener.receipts))
       listener.reset()
       return conn, listener

   def setUp(self):
        self.conn = self.create_connection()
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
        self.latch = Latch(1)


    def on_receipt(self, headers, message):
        self.receipts.append({'message' : message, 'headers' : headers})
        self.latch.countdown()

    def on_error(self, headers, message):
        self.errors.append({'message' : message, 'headers' : headers})
        self.latch.countdown()

    def on_message(self, headers, message):
        self.messages.append({'message' : message, 'headers' : headers})
        self.latch.countdown()

    def reset(self,count=1):
        self.messages = []
        self.errors = []
        self.latch = Latch(count)

    def await(self, timeout=10):
        return self.latch.await(timeout)

class Latch(object):

   def __init__(self, count=1):
      self.cond = threading.Condition()
      self.cond.acquire()
      self.count = count
      self.cond.release()

   def countdown(self):
      self.cond.acquire()
      self.count -= 1
      if self.count == 0:
         self.cond.notify_all()
      self.cond.release()

   def await(self, timeout=None):
      try:
         self.cond.acquire()
         self.cond.wait(timeout)
         return self.count == 0
      finally:
         self.cond.release()

