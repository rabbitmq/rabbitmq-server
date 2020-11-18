#!/usr/bin/env python

import pika
import time

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
ch   = conn.channel()

args = {u'hash-header': u'hash-on'}
ch.exchange_declare(exchange='e2',
                    exchange_type='x-consistent-hash',
                    arguments=args,
                    durable=True)

for q in ['q1', 'q2', 'q3', 'q4']:
    ch.queue_declare(queue=q, durable=True)
    ch.queue_purge(queue=q)

for q in ['q1', 'q2']:
    ch.queue_bind(exchange='e2', queue=q, routing_key='1')

for q in ['q3', 'q4']:
    ch.queue_bind(exchange='e2', queue=q, routing_key='2')

n = 100000

for rk in list(map(lambda s: str(s), range(0, n))):
    hdrs = {u'hash-on': rk}
    ch.basic_publish(exchange='e2',
                     routing_key='',
                     body='',
                     properties=pika.BasicProperties(content_type='text/plain',
                                                     delivery_mode=2,
                                                     headers=hdrs))
print('Done publishing.')

print('Waiting for routing to finish...')
# in order to keep this example simpler and focused,
# wait for a few seconds instead of using publisher confirms and waiting for those
time.sleep(5)

print('Done.')
conn.close()
