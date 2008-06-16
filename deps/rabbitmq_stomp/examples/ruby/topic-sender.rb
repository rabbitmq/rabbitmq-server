require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
client.send 'x.x', 'first message', :exchange => 'amq.topic'
client.send 'y.y', 'second message', :exchange => 'amq.topic'
client.send 'x.y', 'third message', :exchange => 'amq.topic'
