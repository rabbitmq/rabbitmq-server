require 'rubygems'
require 'stomp'

# Use this case to test durable queues
#
# Start the sender    - 11 messages will be sent to /queue/durable and the sender exits
# Start the receiver  - 0 messages should be received - interrupt the receive loop, 
#                       The queue should be now be created as durable and non auto-delete
# Start the sender    - 11 messages will be sent to /queue/durable and the sender exits
# Start the receiver  - 11 messages should be received and the receiver - interrupt the receive loop

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
10.times { |i| client.send '/queue/durable', "Test Message number #{i} sent at #{Time.now}", 'delivery-mode' => '2'}
client.send '/queue/durable', "All Done!"
