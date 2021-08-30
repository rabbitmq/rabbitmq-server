require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
10000.times { |i| client.publish '/queue/carl', "Test Message number #{i}"}
client.publish '/queue/carl', "All Done!"
