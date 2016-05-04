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


defmodule AddVhostCommand do

  @flags []

  def add_vhost([], _), do: {:not_enough_args, []}
  def add_vhost([_|_] = args, _) when length(args) > 1, do: {:too_many_args, args}
  def add_vhost([arg] = args, %{node: node_name} = opts) do
    info(args, opts)
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_vhost, :add, [arg])
  end

  def usage, do: "add_vhost <vhost>"

  defp info(_, %{quiet: true}), do: nil
  defp info([arg], _), do: IO.puts "Adding vhost \"#{arg}\" ..."

  def flags, do: @flags
end
