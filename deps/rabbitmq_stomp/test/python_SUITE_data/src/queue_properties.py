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

class TestQueueProperties(base.BaseTest):

    def test_subscribe(self):
        destination = "/queue/queue-properties-subscribe-test"

        # subscribe
        self.subscribe_dest(self.conn, destination, None,
                            headers={
                                'x-message-ttl': 60000,
                                'x-expires': 70000,
                                'x-max-length': 10,
                                'x-max-length-bytes': 20000,
                                'x-dead-letter-exchange': 'dead-letter-exchange',
                                'x-dead-letter-routing-key': 'dead-letter-routing-key',
                                'x-max-priority': 6,
                                })

        # now try to declare the queue using pika
        # if the properties are the same we should
        # not get any error
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host='127.0.0.1', port=int(os.environ["AMQP_PORT"])))
        channel = connection.channel()
        channel.queue_declare(queue='queue-properties-subscribe-test',
                              durable=True,
                              arguments={
                                  'x-message-ttl': 60000,
                                  'x-expires': 70000,
                                  'x-max-length': 10,
                                  'x-max-length-bytes': 20000,
                                  'x-dead-letter-exchange': 'dead-letter-exchange',
                                  'x-dead-letter-routing-key': 'dead-letter-routing-key',
                                  'x-max-priority': 6,
                                  })

        self.conn.disconnect()
        connection.close()

    def test_send(self):
        destination = "/queue/queue-properties-send-test"

        # send
        self.conn.send(destination, "test1",
                       headers={
                           'x-message-ttl': 60000,
                           'x-expires': 70000,
                           'x-max-length': 10,
                           'x-max-length-bytes': 20000,
                           'x-dead-letter-exchange': 'dead-letter-exchange',
                           'x-dead-letter-routing-key': 'dead-letter-routing-key',
                           'x-max-priority': 6,
                           })

        # now try to declare the queue using pika
        # if the properties are the same we should
        # not get any error
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host='127.0.0.1', port=int(os.environ["AMQP_PORT"])))
        channel = connection.channel()
        channel.queue_declare(queue='queue-properties-send-test',
                              durable=True,
                              arguments={
                                  'x-message-ttl': 60000,
                                  'x-expires': 70000,
                                  'x-max-length': 10,
                                  'x-max-length-bytes': 20000,
                                  'x-dead-letter-exchange': 'dead-letter-exchange',
                                  'x-dead-letter-routing-key': 'dead-letter-routing-key',
                                  'x-max-priority': 6,
                                  })

        self.conn.disconnect()
        connection.close()


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)