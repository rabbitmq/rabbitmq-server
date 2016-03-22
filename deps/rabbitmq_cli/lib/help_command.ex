## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule HelpCommand do

  def help(_, _), do: help
  def help() do
    print_base_usage
    print_commands
    print_input_types
    :ok
  end

  def usage(), do: "help"

  defp print_base_usage() do
    IO.puts "Usage:
rabbitmqctl [-n <node>] [-t <timeout>] [-q] <command> [<command options>] 

Options:
    -n node
    -q
    -t timeout

Default node is \"rabbit@server\", where server is the local host. On a host 
named \"server.example.com\", the node name of the RabbitMQ Erlang node will 
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some 
non-default value at broker startup time). The output of hostname -s is usually 
the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for 
details of configuring the RabbitMQ broker.

Quiet output mode is selected with the \"-q\" flag. Informational messages are 
suppressed when quiet mode is in effect.

Operation timeout in seconds. Only applicable to \"list\" commands. Default is 
\"infinity\".\n"
  end

  defp print_commands() do
    IO.puts "Commands:"

    # Enum.map obtains the usage string for each command module.
    # Enum.each prints them all.
    Helpers.commands
    |>  Map.values
    |>  Enum.map(fn(module) ->
          Code.eval_string("    #{module}.usage")
          |> elem(0)
        end)
    |>  Enum.each(fn(cmd_usage) -> IO.puts "    #{cmd_usage}" end)

    :ok
  end

  defp print_input_types() do
    IO.puts "\n<vhostinfoitem> must be a member of the list [name, tracing].

The list_queues, list_exchanges and list_bindings commands accept an optional 
virtual host parameter for which to display results. The default value is \"/\".

<queueinfoitem> must be a member of the list [name, durable, auto_delete, 
arguments, policy, pid, owner_pid, exclusive, exclusive_consumer_pid, 
exclusive_consumer_tag, messages_ready, messages_unacknowledged, messages, 
messages_ready_ram, messages_unacknowledged_ram, messages_ram, 
messages_persistent, message_bytes, message_bytes_ready, 
message_bytes_unacknowledged, message_bytes_ram, message_bytes_persistent, 
head_message_timestamp, disk_reads, disk_writes, consumers, 
consumer_utilisation, memory, slave_pids, synchronised_slave_pids, state].

<exchangeinfoitem> must be a member of the list [name, type, durable, 
auto_delete, internal, arguments, policy].

<bindinginfoitem> must be a member of the list [source_name, source_kind, 
destination_name, destination_kind, routing_key, arguments].

<connectioninfoitem> must be a member of the list [pid, name, port, host, 
peer_port, peer_host, ssl, ssl_protocol, ssl_key_exchange, ssl_cipher, 
ssl_hash, peer_cert_subject, peer_cert_issuer, peer_cert_validity, state, 
channels, protocol, auth_mechanism, user, vhost, timeout, frame_max, 
channel_max, client_properties, recv_oct, recv_cnt, send_oct, send_cnt, 
send_pend, connected_at].

<channelinfoitem> must be a member of the list [pid, connection, name, number, 
user, vhost, transactional, confirm, consumer_count, messages_unacknowledged, 
messages_uncommitted, acks_uncommitted, messages_unconfirmed, prefetch_count, 
global_prefetch_count].


"
  end
end
