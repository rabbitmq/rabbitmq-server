#!/usr/bin/python
#
# Tests for the SLA patch which adds the head_message_timestamp queue stat.
# Uses both the management interface via rabbitmqadmin and the AMQP interface via Pika.
# There's no particular reason to have used rabbitmqadmin other than saving some bulk.
# Similarly, the separate declaration of exchanges and queues is just a preference
# following a typical enterprise policy where admin users create these resources.

from datetime import datetime
import json
import pika
import os
import sys
from time import clock, mktime, sleep
import unittest

# Uses the rabbitmqadmin script.
# To be imported this must be given a .py suffix and placed on the Python path
from rabbitmqadmin import *

TEXCH = 'head-message-timestamp-test'
TQUEUE = 'head-message-timestamp-test-queue'

TIMEOUT_SECS = 10

TIMESTAMP1 = mktime(datetime(2010,1,1,12,00,01).timetuple())
TIMESTAMP2 = mktime(datetime(2010,1,1,12,00,02).timetuple())

AMQP_PORT = 99

DELIVERY_MODE = 2
DURABLE = False

def log(msg):
    print("\nINFO: " + msg)

class RabbitTestCase(unittest.TestCase):
    def setUp(self):
        parser.set_conflict_handler('resolve')
        (options, args) = make_configuration() 
        AMQP_PORT =  int(options.port) - 10000

        self.mgmt = Management(options, args)
        self.mgmt.put('/exchanges/%2f/' + TEXCH, '{"type" : "fanout", "durable":' + str(DURABLE).lower() + '}')
        self.mgmt.put('/queues/%2f/' + TQUEUE, '{"auto_delete":false,"durable":' + str(DURABLE).lower() + ',"arguments":[]}') 
        self.mgmt.post('/bindings/%2f/e/' + TEXCH + '/q/' + TQUEUE, '{"routing_key": ".*", "arguments":[]}')
        self.credentials = pika.PlainCredentials(options.username, options.password)
        parameters =  pika.ConnectionParameters(options.hostname, port=AMQP_PORT, credentials=self.credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def tearDown(self):
        parser.set_conflict_handler('resolve')
        (options, args) = make_configuration()
        self.mgmt = Management(options, args)
        self.mgmt.delete('/queues/%2f/' + TQUEUE)
        self.mgmt.delete('/exchanges/%2f/' + TEXCH)

class RabbitSlaTestCase(RabbitTestCase):
    def get_queue_stats(self, queue_name):
        stats_str = self.mgmt.get('/queues/%2f/' + queue_name)
        return json.loads(stats_str)

    def get_head_message_timestamp(self, queue_name):
        return self.get_queue_stats(queue_name)["head_message_timestamp"]

    def send(self, message, timestamp=None):
        self.channel.basic_publish(TEXCH, '', message,
                                   pika.BasicProperties(content_type='text/plain',
                                                        delivery_mode=DELIVERY_MODE,
                                                        timestamp=timestamp))
        log("Sent message with body: " + str(message))

    def receive(self, queue):
        method_frame, header_frame, body = self.channel.basic_get(queue = queue)
        log("Received message with body: " + str(body))
        return method_frame.delivery_tag, body

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag)        

    def nack(self, delivery_tag):
        self.channel.basic_nack(delivery_tag)        

    def wait_for_new_timestamp(self, queue, old_timestamp):
        stats_wait_start = clock()
        while ((clock() - stats_wait_start) < TIMEOUT_SECS and
               self.get_head_message_timestamp(queue) == old_timestamp):
            sleep(0.1)
        log('Queue stats updated in ' + str(clock() - stats_wait_start) + ' secs.')
        return self.get_head_message_timestamp(queue)

    # TESTS

    def test_no_timestamp_when_queue_is_empty(self):
        assert self.get_head_message_timestamp(TQUEUE) == ''

    def test_has_timestamp_when_first_msg_is_added(self):
        self.send('Msg1', TIMESTAMP1)
        stats_timestamp = self.wait_for_new_timestamp(TQUEUE, '')
        assert stats_timestamp == TIMESTAMP1
        
    def test_no_timestamp_when_last_msg_is_removed(self):
        self.send('Msg1', TIMESTAMP1)
        stats_timestamp = self.wait_for_new_timestamp(TQUEUE, '')
        tag, body = self.receive(TQUEUE)
        self.ack(tag)
        stats_timestamp = self.wait_for_new_timestamp(TQUEUE, TIMESTAMP1)
        assert stats_timestamp == ''

    def test_timestamp_updated_when_msg_is_removed(self):
        self.send('Msg1', TIMESTAMP1)
        stats_timestamp = self.wait_for_new_timestamp(TQUEUE, '')
        self.send('Msg2', TIMESTAMP2)
        tag, body = self.receive(TQUEUE)
        self.ack(tag)
        stats_timestamp = self.wait_for_new_timestamp(TQUEUE, TIMESTAMP1)
        assert stats_timestamp == TIMESTAMP2

    def test_timestamp_not_updated_before_msg_is_acked(self):
        self.send('Msg1', TIMESTAMP1)
        stats_timestamp = self.wait_for_new_timestamp(TQUEUE, '')
        tag, body = self.receive(TQUEUE)
        sleep(1) # Allow time for update to appear if it was going to (it shouldn't)
        assert self.get_head_message_timestamp(TQUEUE) == TIMESTAMP1
        self.ack(tag) 

if __name__ == '__main__':
    unittest.main(verbosity = 2)
    

