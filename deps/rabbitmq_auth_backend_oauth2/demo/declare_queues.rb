#!/usr/bin/env ruby
require 'bunny'

queues = ARGV

queues.each do |q|
    split = q.split("/")
    vhost = split[0]
    queue_name = split[1]

    conn = Bunny.new(:vhost => vhost)
    conn.start
    ch = conn.create_channel
    ch.queue(queue_name)
    conn.stop
end
