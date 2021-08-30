require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open("guest", "guest", "localhost")
conn.subscribe '/exchange/amq.fanout/test'

puts "Waiting for messages..."

begin
  while mesg = conn.receive
    puts mesg.body
  end
rescue Exception => _
  conn.disconnect
end
