require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
conn.subscribe('carl', {:prefetch => 1, :ack => 'client'})
while mesg = conn.receive
  puts mesg.body
  puts 'Sleeping...'
  sleep 0.2
  puts 'Awake again. Acking.'
  conn.ack mesg.headers['message-id']
end
