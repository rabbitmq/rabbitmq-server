require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
conn.subscribe('/queue/carl')
while mesg = conn.receive
  puts mesg.body
end
