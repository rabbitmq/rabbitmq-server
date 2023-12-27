import time
import sys

import stomp
import random
import requests

class MyListener(stomp.ConnectionListener):
  def on_error(self, frame):
    print('received an error "%s"' % frame.body)
  def on_message(self, frame):
    print('received a message "%s"' % frame.body)

# Define a STOMP connection and port
conn = stomp.Connection([("localhost", 61613)])
conn.set_listener('', MyListener())
conn.connect('guest', 'guest', wait=True) # define the username/password

# Setup a subscription
conn.subscribe(destination='/exchange/stomp1', id=1234, ack='client', headers={
    'x-queue-name': 'my-stomp-stream',
    'x-queue-type': 'stream',
    'x-max-age' : '10h',
    'durable': True,
    'auto-delete': False,
    'id': 1234,
    'prefetch-count': 10
})

response = requests.get("http://localhost:15672/api/queues/%2F/my-stomp-stream", auth=("guest", "guest"))
stream = response.json()
print("Stream arguments:")
print("  x-queue-type:" + stream["arguments"]["x-queue-type"])
print("  x-max-age:" + stream["arguments"]["x-max-age"])

while True:
  time.sleep(15) # send a random message every 15 seconds
  conn.send( destination='/exchange/stomp1', body=str(random.randint(1,11)))

conn.disconnect()
