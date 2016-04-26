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


defmodule AddUserCommand do

  def add_user([], _) do
    {:not_enough_args, []}
  end

  def add_user([arg], _) do
    {:not_enough_args, [arg]} 
  end

  def add_user([_|_] = args, _) when length(args) > 2 do
    {:too_many_args, args}
  end

  def add_user(["", password], _) do
    IO.puts "Error: user cannot be empty string."
    IO.puts "\tGiven: add_user '' #{password}"
    IO.puts "\tUsage: #{usage}"
    {:bad_argument, [""]}
  end

  def add_user([_, _] = args, %{node: node_name}) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(
      :rabbit_auth_backend_internal,
      :add_user,
      args
    )
  end

  def usage, do: "add_user <username> <password>"
end
