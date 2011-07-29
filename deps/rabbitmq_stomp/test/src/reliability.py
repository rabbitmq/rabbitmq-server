import base
import stomp
import unittest

class TestReliability(base.BaseTest):

    def test_send_and_disconnect(self):
        ''' Test close socket after send does not lose messages '''
        d = "/queue/reliability"
        pub_conn = self.create_connection()
        try:
            msg = "0" * (128)

            count = 10000

            listener = base.WaitableListener()
            listener.reset(count)
            self.conn.set_listener('', listener)
            self.conn.subscribe(destination=d)

            for x in range(0, count):
                pub_conn.send(msg + str(x), destination=d)

            pub_conn.close_socket()
            self.assertTrue(listener.await(30))
            self.assertEquals(count, len(listener.messages))
        finally:
            if pub_conn.is_connected():
                pub_conn.disconnect()
