#!/usr/bin/env python

import amqplib.client_0_8 as amqp

def callback(msg):
  print (msg.body)
  msg.channel.basic_ack(msg.delivery_tag)
  
def main():
  conn = amqp.Connection()
  channel = conn.channel()
  exch = channel.exchange_declare("randomtest", "random", auto_delete=False)
  
  q, _, _ = channel.queue_declare()
  channel.queue_bind(q, "randomtest", "random")
  channel.basic_consume(q, callback=callback)
  
  while channel.callbacks:
    channel.wait()
    
  channel.close()
  conn.close()

if __name__ == '__main__':
  main()