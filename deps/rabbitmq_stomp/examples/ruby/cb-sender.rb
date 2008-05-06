require 'rubygems'
require 'stomp'

client = Stomp::Client.new("guest", "guest", "localhost", 61613)
10000.times { |i| client.send 'carl', "Test Message number #{i}"}
client.send 'carl', "All Done!"
