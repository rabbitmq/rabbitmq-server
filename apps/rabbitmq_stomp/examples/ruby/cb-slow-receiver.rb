require 'rubygems'
require 'stomp'

# Note: requires support for connect_headers hash in the STOMP gem's connection.rb
conn = Stomp::Connection.open('guest', 'guest', 'localhost', 61613, false, 5, {:prefetch => 1})
conn.subscribe('/queue/carl', {:ack => 'client'})
while mesg = conn.receive
  puts mesg.body
  puts 'Sleeping...'
  sleep 0.2
  puts 'Awake again. Acking.'
  conn.ack mesg.headers['message-id']
end
