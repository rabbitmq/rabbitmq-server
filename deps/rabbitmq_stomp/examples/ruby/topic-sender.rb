require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
client.publish '/topic/x.y', 'first message'
client.publish '/topic/x.z', 'second message'
client.publish '/topic/x', 'third message'
