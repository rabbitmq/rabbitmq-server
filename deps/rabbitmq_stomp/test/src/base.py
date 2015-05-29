import unittest
import stomp
import sys
import threading


class BaseTest(unittest.TestCase):

   def create_connection_obj(self, version='1.0', vhost='/', heartbeats=(0, 0)):
       if version == '1.0':
           conn = stomp.StompConnection10()
           self.ack_id_source_header = 'message-id'
           self.ack_id_header = 'message-id'
       elif version == '1.1':
           conn = stomp.StompConnection11(vhost=vhost,
                                          heartbeats=heartbeats)
           self.ack_id_source_header = 'message-id'
           self.ack_id_header = 'message-id'
       elif version == '1.2':
           conn = stomp.StompConnection12(vhost=vhost,
                                          heartbeats=heartbeats)
           self.ack_id_source_header = 'ack'
           self.ack_id_header = 'id'
       else:
           conn = stomp.StompConnection12(vhost=vhost,
                                          heartbeats=heartbeats)
           conn.version = version
       return conn

   def create_connection(self, user='guest', passcode='guest', wait=True, **kwargs):
       conn = self.create_connection_obj(**kwargs)
       conn.start()
       conn.connect(user, passcode, wait=wait)
       return conn

   def subscribe_dest(self, conn, destination, sub_id, **kwargs):
       if type(conn) is stomp.StompConnection10:
           # 'id' is optional in STOMP 1.0.
           if sub_id != None:
               kwargs['id'] = sub_id
           conn.subscribe(destination, **kwargs)
       else:
           # 'id' is required in STOMP 1.1+.
           if sub_id == None:
               sub_id = 'ctag'
           conn.subscribe(destination, sub_id, **kwargs)

   def unsubscribe_dest(self, conn, destination, sub_id, **kwargs):
       if type(conn) is stomp.StompConnection10:
           # 'id' is optional in STOMP 1.0.
           if sub_id != None:
               conn.unsubscribe(id=sub_id, **kwargs)
           else:
               conn.unsubscribe(destination=destination, **kwargs)
       else:
           # 'id' is required in STOMP 1.1+.
           if sub_id == None:
               sub_id = 'ctag'
           conn.unsubscribe(sub_id, **kwargs)

   def ack_message(self, conn, msg_id, sub_id, **kwargs):
       if type(conn) is stomp.StompConnection10:
           conn.ack(msg_id, **kwargs)
       elif type(conn) is stomp.StompConnection11:
           if sub_id == None:
               sub_id = 'ctag'
           conn.ack(msg_id, sub_id, **kwargs)
       elif type(conn) is stomp.StompConnection12:
           conn.ack(msg_id, **kwargs)

   def nack_message(self, conn, msg_id, sub_id, **kwargs):
       if type(conn) is stomp.StompConnection10:
           # Normally unsupported by STOMP 1.0.
           conn.send_frame("NACK", {"message-id": msg_id})
       elif type(conn) is stomp.StompConnection11:
           if sub_id == None:
               sub_id = 'ctag'
           conn.nack(msg_id, sub_id, **kwargs)
       elif type(conn) is stomp.StompConnection12:
           conn.nack(msg_id, **kwargs)

   def create_subscriber_connection(self, dest):
       conn = self.create_connection()
       listener = WaitableListener()
       conn.set_listener('', listener)
       self.subscribe_dest(conn, dest, None, receipt="sub.receipt")
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
            self.conn.disconnect()
            self.conn.stop()

   def simple_test_send_rec(self, dest, route = None):
        self.listener.reset()

        self.subscribe_dest(self.conn, dest, None)
        self.conn.send(dest, "foo")

        self.assertTrue(self.listener.await(), "Timeout, no message received")

        # assert no errors
        if len(self.listener.errors) > 0:
            self.fail(self.listener.errors[0]['message'])

        # check header content
        msg = self.listener.messages[0]
        self.assertEquals("foo", msg['message'])
        self.assertEquals(dest, msg['headers']['destination'])

   def assertListener(self, errMsg, numMsgs=0, numErrs=0, numRcts=0, timeout=10):
        if numMsgs + numErrs + numRcts > 0:
            self._assertTrue(self.listener.await(timeout), errMsg + " (#awaiting)")
        else:
            self._assertFalse(self.listener.await(timeout), errMsg + " (#awaiting)")
        self._assertEquals(numMsgs, len(self.listener.messages), errMsg + " (#messages)")
        self._assertEquals(numErrs, len(self.listener.errors), errMsg + " (#errors)")
        self._assertEquals(numRcts, len(self.listener.receipts), errMsg + " (#receipts)")

   def _assertTrue(self, bool, msg):
       if not bool:
           self.listener.print_state(msg, True)
           self.assertTrue(bool, msg)

   def _assertFalse(self, bool, msg):
       if bool:
           self.listener.print_state(msg, True)
           self.assertFalse(bool, msg)

   def _assertEquals(self, expected, actual, msg):
       if expected != actual:
           self.listener.print_state(msg, True)
           self.assertEquals(expected, actual, msg)

   def assertListenerAfter(self, verb, errMsg="", numMsgs=0, numErrs=0, numRcts=0, timeout=5):
        num = numMsgs + numErrs + numRcts
        self.listener.reset(num if num>0 else 1)
        verb()
        self.assertListener(errMsg=errMsg, numMsgs=numMsgs, numErrs=numErrs, numRcts=numRcts, timeout=timeout)

class WaitableListener(object):

    def __init__(self):
        self.debug = False
        if self.debug:
            print '(listener) init'
        self.messages = []
        self.errors = []
        self.receipts = []
        self.latch = Latch(1)
        self.msg_no = 0

    def _next_msg_no(self):
        self.msg_no += 1
        return self.msg_no

    def _append(self, array, msg, hdrs):
        mno = self._next_msg_no()
        array.append({'message' : msg, 'headers' : hdrs, 'msg_no' : mno})
        self.latch.countdown()

    def on_receipt(self, headers, message):
        if self.debug:
            print '(on_receipt) message:', message, 'headers:', headers
        self._append(self.receipts, message, headers)

    def on_error(self, headers, message):
        if self.debug:
            print '(on_error) message:', message, 'headers:', headers
        self._append(self.errors, message, headers)

    def on_message(self, headers, message):
        if self.debug:
            print '(on_message) message:', message, 'headers:', headers
        self._append(self.messages, message, headers)

    def reset(self, count=1):
        if self.debug:
            self.print_state('(reset listener--old state)')
        self.messages = []
        self.errors = []
        self.receipts = []
        self.latch = Latch(count)
        self.msg_no = 0
        if self.debug:
            self.print_state('(reset listener--new state)')

    def await(self, timeout=10):
        return self.latch.await(timeout)

    def print_state(self, hdr="", full=False):
        print hdr,
        print '#messages:', len(self.messages),
        print '#errors:', len(self.errors),
        print '#receipts:', len(self.receipts),
        print 'Remaining count:', self.latch.get_count()
        if full:
            if len(self.messages) != 0: print 'Messages:', self.messages
            if len(self.errors) != 0: print 'Messages:', self.errors
            if len(self.receipts) != 0: print 'Messages:', self.receipts

class Latch(object):

   def __init__(self, count=1):
      self.cond = threading.Condition()
      self.cond.acquire()
      self.count = count
      self.cond.release()

   def countdown(self):
      self.cond.acquire()
      if self.count > 0:
         self.count -= 1
      if self.count == 0:
         self.cond.notify_all()
      self.cond.release()

   def await(self, timeout=None):
      try:
         self.cond.acquire()
         if self.count == 0:
            return True
         else:
            self.cond.wait(timeout)
            return self.count == 0
      finally:
         self.cond.release()

   def get_count(self):
      try:
         self.cond.acquire()
         return self.count
      finally:
         self.cond.release()
