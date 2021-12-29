## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import pika
import base
import os

class TestAmqpHeaders(base.BaseTest):
    def test_headers_to_stomp(self):
        self.listener.reset(1)
        queueName='test-amqp-headers-to-stomp'

        # Set up STOMP subscription
        self.subscribe_dest(self.conn, '/topic/test', None, headers={'x-queue-name': queueName})

        # Set up AMQP connection
        amqp_params = pika.ConnectionParameters(host='localhost', port=int(os.environ["AMQP_PORT"]))
        amqp_conn = pika.BlockingConnection(amqp_params)
        amqp_chan = amqp_conn.channel()

        # publish a message with headers to the named AMQP queue
        amqp_headers = { 'x-custom-hdr-1': 'value1',
                         'x-custom-hdr-2': 'value2',
                         'custom-hdr-3': 'value3' }
        amqp_props = pika.BasicProperties(headers=amqp_headers)
        amqp_chan.basic_publish(exchange='', routing_key=queueName, body='Hello World!', properties=amqp_props)

        # check if we receive the message from the STOMP subscription
        self.assertTrue(self.listener.wait(2), "initial message not received")
        self.assertEqual(1, len(self.listener.messages))
        msg = self.listener.messages[0]
        self.assertEqual('Hello World!', msg['message'])
        self.assertEqual('value1', msg['headers']['x-custom-hdr-1'])
        self.assertEqual('value2', msg['headers']['x-custom-hdr-2'])
        self.assertEqual('value3', msg['headers']['custom-hdr-3'])

        self.conn.disconnect()
        amqp_conn.close()


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)