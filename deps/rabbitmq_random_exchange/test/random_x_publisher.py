#!/usr/bin/env python

import amqplib.client_0_8 as amqp

def main():
  conn = amqp.Connection()
  channel = conn.channel()
  exch = channel.exchange_declare("test", "x-random", auto_delete=False)
  
  msg = amqp.Message("hello world!")
  channel.basic_publish(msg, "test", "test")
    
  channel.close()
  conn.close()

if __name__ == '__main__':
  main()