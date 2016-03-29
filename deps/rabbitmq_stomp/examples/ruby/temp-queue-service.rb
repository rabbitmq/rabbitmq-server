require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open("guest", "guest", "localhost")
conn.subscribe '/queue/rpc-service'

begin
  while mesg = conn.receive
    puts "received message and replies to #{mesg.headers['reply-to']}"

    conn.publish(mesg.headers['reply-to'], '(reply) ' + mesg.body)
  end
rescue Exception => _
  conn.disconnect
end
