## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
##

import pika
import base
import time
import os
import re


class TestUserGeneratedQueueName(base.BaseTest):

    def test_stream_queue(self):
        queueName = 'my-stream-queue'

        # subscribe
        self.subscribe_dest(
                self.conn,
                '/topic/stream-queue-test',
                None,
                headers={
                    'x-queue-name': queueName,
                    'x-queue-type': 'stream',
                    'durable': True,
                    'auto-delete': False,
                    'id': 1234,
                    'prefetch-count': 10
                },
               ack="client"
                )

        # let the stream queue some time to start
        time.sleep(5)

        connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='127.0.0.1', port=int(os.environ["AMQP_PORT"])))
        channel = connection.channel()

        # publish a message to the named queue
        channel.basic_publish(
                exchange='',
                routing_key=queueName,
                body='Hello World!')

        # could we declare a stream queue?
        stream_queue_supported = True
        if len(self.listener.errors) > 0:
            pattern = re.compile(r"feature flag is disabled", re.MULTILINE)
            for error in self.listener.errors:
                if pattern.search(error['message']) != None:
                    stream_queue_supported = False
                    break

        if stream_queue_supported:
            # check if we receive the message from the STOMP subscription
            self.assertTrue(self.listener.wait(5), "initial message not received")
            self.assertEqual(1, len(self.listener.messages))
            self.conn.disconnect()

        connection.close()


if __name__ == '__main__':
    import test_runner
    modules = [
        __name__
    ]
    test_runner.run_unittests(modules)