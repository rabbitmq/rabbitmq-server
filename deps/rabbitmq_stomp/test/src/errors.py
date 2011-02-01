import unittest
import stomp
import base
import time

class TestErrors(base.BaseTest):

    def test_invalid_queue_destination(self):
        self.__test_invalid_destination("queue", "/bah/baz")

    def test_invalid_empty_queue_destination(self):
        self.__test_invalid_destination("queue", "")

    def test_invalid_topic_destination(self):
        self.__test_invalid_destination("topic", "/bah/baz")

    def test_invalid_empty_topic_destination(self):
        self.__test_invalid_destination("topic", "")

    def test_invalid_exchange_destination(self):
        self.__test_invalid_destination("exchange", "/bah/baz/boo")

    def test_invalid_empty_exchange_destination(self):
        self.__test_invalid_destination("exchange", "")

    def test_invalid_default_exchange_destination(self):
        self.__test_invalid_destination("exchange", "//foo")

    def test_unknown_destination(self):
        self.listener.reset()
        self.conn.send(destination="/something/interesting")

        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.errors))

        err = self.listener.errors[0]
        self.assertEquals("Unknown destination", err['headers']['message'])

    def test_send_missing_destination(self):
        self.__test_missing_destination("SEND")

    def test_send_missing_destination(self):
        self.__test_missing_destination("SUBSCRIBE")

    def __test_missing_destination(self, command):
        self.listener.reset()
        self.conn.send_frame(command)

        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.errors))

        err = self.listener.errors[0]
        self.assertEquals("Missing destination", err['headers']['message'])

    def __test_invalid_destination(self, dtype, content):
        self.listener.reset()
        self.conn.send(destination="/" + dtype + content)

        self.assertTrue(self.listener.await())
        self.assertEquals(1, len(self.listener.errors))

        err = self.listener.errors[0]
        self.assertEquals("Invalid destination", err['headers']['message'])
        self.assertEquals("'" + content + "' is not a valid " +
                              dtype + " destination\n",
                          err['message'])

