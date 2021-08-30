#!/usr/bin/env ruby

require 'bundler'
Bundler.setup(:default, :test)
require 'bunny'

conn = Bunny.new
conn.start

ch = conn.create_channel
ch.confirm_select

q1 = ch.queue("q1", durable: true)
q2 = ch.queue("q2", durable: true)
q3 = ch.queue("q3", durable: true)
q4 = ch.queue("q4", durable: true)

[q1, q2, q3, q4].each(&:purge)

x  = ch.exchange("x3", type: "x-consistent-hash", durable: true, arguments: {"hash-property" => "message_id"})

[q1, q2].each { |q| q.bind(x, routing_key: "1") }
[q3, q4].each { |q| q.bind(x, routing_key: "2") }

n = 100_000
(0..n).map(&:to_s).each do |i|
  x.publish(i.to_s, routing_key: rand.to_s, message_id: i)
end

ch.wait_for_confirms
puts "Done publishing!"

# wait for queue stats to be emitted so that management UI numbers
# are up-to-date
sleep 5
conn.close
puts "Done"
