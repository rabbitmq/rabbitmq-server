require 'rubygems'
require 'stomp'

topic = ARGV[0] || 'x'
puts "Binding to /topic/#{topic}"

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
conn.subscribe("/topic/#{topic}")
while mesg = conn.receive
  puts mesg.body
end
