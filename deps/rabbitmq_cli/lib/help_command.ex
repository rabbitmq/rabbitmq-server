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
  import Helpers

  def help() do
    print_base_usage
    print_commands
  end

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
    IO.puts "status"
  end
end
