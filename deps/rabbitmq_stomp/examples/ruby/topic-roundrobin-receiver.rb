require 'rubygems'
require 'stomp'

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
conn.subscribe('myqueue', :exchange => 'amq.topic', :routing_key => "x.#")
while mesg = conn.receive
  puts mesg.body
end
