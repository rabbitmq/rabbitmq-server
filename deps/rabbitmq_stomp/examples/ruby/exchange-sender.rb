require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)

# This publishes a message to the 'amq.fanout' exchange which is managed by 
# AMQP broker and specifies routing-key of 'test'. You can get other exchanges
# through 'list_exchanges' subcommand of 'rabbitmqctl' utility.
client.publish("/exchange/amq.fanout/test", "test message")

# close this connection
client.close
