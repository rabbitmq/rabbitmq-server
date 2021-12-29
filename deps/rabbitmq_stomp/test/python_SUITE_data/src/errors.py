## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import unittest
import stomp
import base
import time

class TestErrorsAndCloseConnection(base.BaseTest):
    def __test_duplicate_consumer_tag_with_headers(self, destination, headers):
        self.subscribe_dest(self.conn, destination, None,
                            headers = headers)

        self.subscribe_dest(self.conn, destination, None,
                            headers = headers)

        self.assertTrue(self.listener.wait())

        self.assertEqual(1, len(self.listener.errors))
        errorReceived = self.listener.errors[0]
        self.assertEqual("Duplicated subscription identifier", errorReceived['headers']['message'])
        self.assertEqual("A subscription identified by 'T_1' already exists.", errorReceived['message'])
        time.sleep(2)
        self.assertFalse(self.conn.is_connected())


    def test_duplicate_consumer_tag_with_transient_destination(self):
        destination = "/exchange/amq.direct/duplicate-consumer-tag-test1"
        self.__test_duplicate_consumer_tag_with_headers(destination, {'id': 1})

    def test_duplicate_consumer_tag_with_durable_destination(self):
        destination = "/queue/duplicate-consumer-tag-test2"
        self.__test_duplicate_consumer_tag_with_headers(destination, {'id': 1,
                                                                      'persistent': True})


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
        self.conn.send("/something/interesting", 'test_unknown_destination')

        self.assertTrue(self.listener.wait())
        self.assertEqual(1, len(self.listener.errors))

        err = self.listener.errors[0]
        self.assertEqual("Unknown destination", err['headers']['message'])

    def test_send_missing_destination(self):
        self.__test_missing_destination("SEND")

    def test_send_missing_destination(self):
        self.__test_missing_destination("SUBSCRIBE")

    def __test_missing_destination(self, command):
        self.listener.reset()
        self.conn.send_frame(command)

        self.assertTrue(self.listener.wait())
        self.assertEqual(1, len(self.listener.errors))

        err = self.listener.errors[0]
        self.assertEqual("Missing destination", err['headers']['message'])

    def __test_invalid_destination(self, dtype, content):
        self.listener.reset()
        self.conn.send("/" + dtype + content, '__test_invalid_destination:' + dtype + content)

        self.assertTrue(self.listener.wait())
        self.assertEqual(1, len(self.listener.errors))

        err = self.listener.errors[0]
        self.assertEqual("Invalid destination", err['headers']['message'])
        self.assertEqual("'" + content + "' is not a valid " +
                              dtype + " destination\n",
                          err['message'])


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)