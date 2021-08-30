## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import unittest
import stomp
import pika
import base
import time
import os

class TestUserGeneratedQueueName(base.BaseTest):

    def test_exchange_dest(self):
        queueName='my-user-generated-queue-name-exchange'

        # subscribe
        self.subscribe_dest(
                self.conn,
                '/exchange/amq.direct/test',
                None,
                headers={ 'x-queue-name': queueName }
                )

        connection = pika.BlockingConnection(
                pika.ConnectionParameters( host='127.0.0.1', port=int(os.environ["AMQP_PORT"])))
        channel = connection.channel()

        # publish a message to the named queue
        channel.basic_publish(
                exchange='',
                routing_key=queueName,
                body='Hello World!')

        # check if we receive the message from the STOMP subscription
        self.assertTrue(self.listener.wait(2), "initial message not received")
        self.assertEqual(1, len(self.listener.messages))

        self.conn.disconnect()
        connection.close()

    def test_topic_dest(self):
        queueName='my-user-generated-queue-name-topic'

        # subscribe
        self.subscribe_dest(
                self.conn,
                '/topic/test',
                None,
                headers={ 'x-queue-name': queueName }
                )

        connection = pika.BlockingConnection(
                pika.ConnectionParameters( host='127.0.0.1', port=int(os.environ["AMQP_PORT"])))
        channel = connection.channel()

        # publish a message to the named queue
        channel.basic_publish(
                exchange='',
                routing_key=queueName,
                body='Hello World!')

        # check if we receive the message from the STOMP subscription
        self.assertTrue(self.listener.wait(2), "initial message not received")
        self.assertEqual(1, len(self.listener.messages))

        self.conn.disconnect()
        connection.close()


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)