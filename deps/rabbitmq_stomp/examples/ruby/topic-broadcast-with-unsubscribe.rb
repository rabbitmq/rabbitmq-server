require 'rubygems'
require 'stomp' # this is a gem
require 'uuidtools' # this is a gem

conn = Stomp::Connection.open('guest', 'guest', 'localhost')
subid = UUID.random_create.to_s
puts "Subscribing to #{subid}..."
conn.subscribe('', :id => subid, :exchange => 'amq.topic', :routing_key => "x.#")
puts 'Receiving...'
mesg = conn.receive
puts mesg.body
puts "Unsubscribing from #{subid}..."
conn.unsubscribe('', :id => subid)
puts 'Sleeping 5 seconds...'
sleep 5
