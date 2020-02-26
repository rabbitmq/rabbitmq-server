import unittest
import stomp
import pika
import base
import time
import os

class TestDeclareExchange(base.BaseTest):

    def test_subscribe(self):
        destination = "/exchange/declare-exchange-subscribe-test"

        # subscribe
        self.subscribe_dest(self.conn, destination, None,
                            receipt='subscribed',
                            headers={
                                'declare-exchange': True,
                                })

        self.assertListener("Couldn't declare exchange", numRcts=1)

    def test_send(self):
        destination = "/exchange/declare-exchange-send-test"

        # send
        self.conn.send(destination, "test1",
                       receipt='sent',
                       headers={
                           'declare-exchange': True,
                           })

        self.assertListener("Couldn't declare exchange", numRcts=1)

    def test_properties(self):
        destination = "/exchange/declare-exchange-properties-test"

        # subscribe
        self.subscribe_dest(self.conn, destination, None,
                            receipt='subscribed',
                            headers={
                                'declare-exchange': True,
                                'exchange-type': 'topic',
                                'exchange-durable': False,
                                'exchange-auto-delete': True,
                                })

        self.assertListener("Couldn't declare exchange", numRcts=1)

        # now try to declare the queue using pika
        # if the properties are the same we should
        # not get any error
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host='127.0.0.1', port=int(os.environ["AMQP_PORT"])))
        channel = connection.channel()
        channel.exchange_declare(exchange='declare-exchange-properties-test',
                                 exchange_type='topic',
                                 durable=False,
                                 auto_delete=True)

        connection.close()
