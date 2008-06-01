require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
#conn.subscribe('/queue/durable', :'auto-delete' => false, :durable => true)
conn.subscribe('/queue/durable')

puts "Waiting for messages..."

while true
  mesg = conn.receive
  puts mesg.body if mesg
end
