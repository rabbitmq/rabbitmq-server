#!/usr/bin/env python

import amqplib.client_0_8 as amqp

def main():
  conn = amqp.Connection()
  channel = conn.channel()
  exch = channel.exchange_declare("randomtest", "random", auto_delete=False)
  
  for i in range(100):
    msg = amqp.Message("hello world! %s" % i)
    channel.basic_publish(msg, "randomtest", "random")
    
  channel.close()
  conn.close()

if __name__ == '__main__':
  main()