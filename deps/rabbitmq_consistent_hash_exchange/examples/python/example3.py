#!/usr/bin/env python

import pika
import time

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
ch   = conn.channel()

args = {u'hash-property': u'message_id'}
ch.exchange_declare(exchange='e3',
                    exchange_type='x-consistent-hash',
                    arguments=args,
                    durable=True)

for q in ['q1', 'q2', 'q3', 'q4']:
    ch.queue_declare(queue=q, durable=True)
    ch.queue_purge(queue=q)

for q in ['q1', 'q2']:
    ch.queue_bind(exchange='e3', queue=q, routing_key='1')

for q in ['q3', 'q4']:
    ch.queue_bind(exchange='e3', queue=q, routing_key='2')

n = 100000

for rk in list(map(lambda s: str(s), range(0, n))):
    ch.basic_publish(exchange='e3',
                     routing_key='',
                     body='',
                     properties=pika.BasicProperties(content_type='text/plain',
                                                     delivery_mode=2,
                                                     message_id=rk))
print('Done publishing.')

print('Waiting for routing to finish...')
# in order to keep this example simpler and focused,
# wait for a few seconds instead of using publisher confirms and waiting for those
time.sleep(5)

print('Done.')
conn.close()
