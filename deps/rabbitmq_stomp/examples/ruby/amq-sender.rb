require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)

# This publishes a message to a queue named 'amq-test' which is managed by AMQP broker.
client.publish("/amq/queue/amq-test", "test-message")

# close this connection
client.close
