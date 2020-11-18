#!/usr/bin/env ruby
require 'bunny'

queues = ARGV

queues.each do |q|
    split = q.split("/")
    vhost = split[0]
    queue_name = split[1]

    conn = Bunny.new(:host => ENV["BUNNY_HOST"] || "127.0.0.1",
                     :user => ENV["BUNNY_USER"] || "guest",
                     :pass => ENV["BUNNY_PASS"] || "guest",
                     :vhost => vhost)
    conn.start
    ch = conn.create_channel
    ch.queue(queue_name)
    conn.stop
end
