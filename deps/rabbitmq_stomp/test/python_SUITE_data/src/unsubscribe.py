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
import threading

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
        d = "/queue/test_unsubscribe_queue_destination"
        self.unsub_test(d, self.sub_and_send(d))

    def test_unsubscribe_queue_destination_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue'''
        d = "/queue/test_unsubscribe_queue_destination_with_receipt"
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
        d = "/queue/test_unsubscribe_queue_id"
        self.unsub_test(d, self.sub_and_send(d, subid="queid"))

    def test_unsubscribe_queue_id_with_receipt(self):
        ''' Test receipted UNSUBSCRIBE command with queue by id'''
        d = "/queue/test_unsubscribe_queue_id_with_receipt"
        self.unsub_test(d, self.sub_and_send(d, subid="queid", receipt="unsub.rct"), numRcts=1)

    ##
    ## Helpers
    ##

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
            time.sleep(1)
            self.conn.send(dest, "test")
        def unsubfun():
            headers = {}
            if receipt != None:
                headers['receipt'] = receipt
            self.unsubscribe_dest(self.conn, dest, subid, **headers)
        return subfun, unsubfun

if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)