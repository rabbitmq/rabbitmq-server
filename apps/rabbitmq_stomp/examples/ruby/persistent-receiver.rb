require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
conn.subscribe('/queue/durable', :'auto-delete' => false, :durable => true)

puts "Waiting for messages..."

while mesg = conn.receive
  puts mesg.body
end
