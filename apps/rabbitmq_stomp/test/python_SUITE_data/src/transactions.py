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

class TestTransactions(base.BaseTest):

    def test_tx_commit(self):
        ''' Test TX with a COMMIT and ensure messages are delivered '''
        destination = "/exchange/amq.fanout"
        tx = "test.tx"

        self.listener.reset()
        self.subscribe_dest(self.conn, destination, None)
        self.conn.begin(transaction=tx)
        self.conn.send(destination, "hello!", transaction=tx)
        self.conn.send(destination, "again!")

        ## should see the second message
        self.assertTrue(self.listener.wait(3))
        self.assertEqual(1, len(self.listener.messages))
        self.assertEqual("again!", self.listener.messages[0]['message'])

        ## now look for the first message
        self.listener.reset()
        self.conn.commit(transaction=tx)
        self.assertTrue(self.listener.wait(3))
        self.assertEqual(1, len(self.listener.messages),
                          "Missing committed message")
        self.assertEqual("hello!", self.listener.messages[0]['message'])

    def test_tx_abort(self):
        ''' Test TX with an ABORT and ensure messages are discarded '''
        destination = "/exchange/amq.fanout"
        tx = "test.tx"

        self.listener.reset()
        self.subscribe_dest(self.conn, destination, None)
        self.conn.begin(transaction=tx)
        self.conn.send(destination, "hello!", transaction=tx)
        self.conn.send(destination, "again!")

        ## should see the second message
        self.assertTrue(self.listener.wait(3))
        self.assertEqual(1, len(self.listener.messages))
        self.assertEqual("again!", self.listener.messages[0]['message'])

        ## now look for the first message to be discarded
        self.listener.reset()
        self.conn.abort(transaction=tx)
        self.assertFalse(self.listener.wait(3))
        self.assertEqual(0, len(self.listener.messages),
                          "Unexpected committed message")

if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)