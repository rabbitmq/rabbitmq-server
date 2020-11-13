require 'rubygems'
require 'stomp'

# Use this case to test durable queues
#
# Start the sender    - 11 messages will be sent to /queue/durable and the sender exits
# Stop the server     - 11 messages will be written to disk
# Start the server
# Start the receiver  - 11 messages should be received and the receiver - interrupt the receive loop

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
10.times { |i| client.publish '/queue/durable', "Test Message number #{i} sent at #{Time.now}", 'delivery-mode' => '2'}
client.publish '/queue/durable', "All Done!"
