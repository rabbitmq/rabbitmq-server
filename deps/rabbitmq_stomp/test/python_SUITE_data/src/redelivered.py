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

class TestRedelivered(base.BaseTest):

    def test_redelivered(self):
        destination = "/queue/redelivered-test"

        # subscribe and send message
        self.subscribe_dest(self.conn, destination, None, ack='client')
        self.conn.send(destination, "test1")
        message_receive_timeout = 30
        self.assertTrue(self.listener.wait(message_receive_timeout), "Test message not received within {0} seconds".format(message_receive_timeout))
        self.assertEqual(1, len(self.listener.messages))
        self.assertEqual('false', self.listener.messages[0]['headers']['redelivered'])

        # disconnect with no ack
        self.conn.disconnect()

        # now reconnect
        conn2 = self.create_connection()
        try:
            listener2 = base.WaitableListener()
            listener2.reset(1)
            conn2.set_listener('', listener2)
            self.subscribe_dest(conn2, destination, None, ack='client')
            self.assertTrue(listener2.wait(), "message not received again")
            self.assertEqual(1, len(listener2.messages))
            self.assertEqual('true', listener2.messages[0]['headers']['redelivered'])
        finally:
            conn2.disconnect()

if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)