#!/usr/bin/env ruby
require 'net/http'
require 'bunny'
require 'yaml'

host = ENV["BUNNY_HOST"]

config = YAML.load_file('/root/.uaac.yml')
access_token = config["http://#{host}:8080/uaa"]["contexts"]["rabbit_super"]["access_token"]

conn = Bunny.new(:host => host,
                 :user => "",
                 :pass => access_token,
                 :vhost => "uaa_vhost")
conn.start
puts "Connected via AMQP!"
conn.stop

uri = URI("http://#{host}:15672/api/vhosts")
req = Net::HTTP::Get.new(uri)
req['Authorization'] = "Bearer #{access_token}"

res = Net::HTTP.start(uri.hostname, uri.port) { |http|
  http.request(req)
}

raise "Could not connect to managment API." unless res.is_a?(Net::HTTPSuccess)
puts "Connected via Management Plugin API!"
puts res.body
