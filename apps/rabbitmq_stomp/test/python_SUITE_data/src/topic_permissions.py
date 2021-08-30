## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import base
import test_util
import sys


class TestTopicPermissions(base.BaseTest):
    @classmethod
    def setUpClass(cls):
        test_util.rabbitmqctl(['set_topic_permissions', 'guest', 'amq.topic', '^{username}.Authorised', '^{username}.Authorised'])
        cls.authorised_topic = '/topic/guest.AuthorisedTopic'
        cls.restricted_topic = '/topic/guest.RestrictedTopic'

    @classmethod
    def tearDownClass(cls):
        test_util.rabbitmqctl(['clear_topic_permissions', 'guest'])

    def test_publish_authorisation(self):
        ''' Test topic permissions via publish '''
        self.listener.reset()

        # send on authorised topic
        self.subscribe_dest(self.conn, self.authorised_topic, None)
        self.conn.send(self.authorised_topic, "authorised hello")

        self.assertTrue(self.listener.wait(), "Timeout, no message received")

        # assert no errors
        if len(self.listener.errors) > 0:
            self.fail(self.listener.errors[0]['message'])

        # check msg content
        msg = self.listener.messages[0]
        self.assertEqual("authorised hello", msg['message'])
        self.assertEqual(self.authorised_topic, msg['headers']['destination'])

        self.listener.reset()

        # send on restricted topic
        self.conn.send(self.restricted_topic, "hello")

        self.assertTrue(self.listener.wait(), "Timeout, no message received")

        # assert errors
        self.assertGreater(len(self.listener.errors), 0)
        self.assertIn("ACCESS_REFUSED", self.listener.errors[0]['message'])


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)