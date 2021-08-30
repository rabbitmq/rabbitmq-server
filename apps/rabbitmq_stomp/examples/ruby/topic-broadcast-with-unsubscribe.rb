require 'rubygems'
require 'stomp' # this is a gem

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
puts "Subscribing to /topic/x"
conn.subscribe('/topic/x')
puts 'Receiving...'
mesg = conn.receive
puts mesg.body
puts "Unsubscribing from /topic/x"
conn.unsubscribe('/topic/x')
puts 'Sleeping 5 seconds...'
sleep 5
