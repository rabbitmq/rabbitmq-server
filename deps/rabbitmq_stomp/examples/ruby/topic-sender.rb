require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
client.publish '/topic/x', 'first message'
client.publish '/topic/x', 'second message'
client.publish '/topic/x', 'third message'
