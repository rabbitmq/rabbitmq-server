require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
conn.subscribe('carl')
while true
  mesg = conn.receive

  puts mesg if mesg

  puts mesg.body if mesg
  sleep 0.0001
end
