require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open("guest", "guest", "localhost")
conn.publish("/queue/rpc-service", "test message", {
  'reply-to' => '/temp-queue/test'
})
puts conn.receive.body
conn.disconnect
