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


defmodule ClearPermissionsCommand do

  @behaviour CommandBehaviour
  @default_vhost "/"
  @flags [:param]

  def run([], _) do
    {:not_enough_args, []}
  end

  def run([_|_] = args, _) when length(args) > 1 do
    {:too_many_args, args}
  end

  def run([username], %{node: node_name, param: vhost} = opts) do
    info(username, opts)
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_auth_backend_internal, :clear_permissions, [username, vhost])
  end

  def run([username], %{node: node_name}) do
    run([username], %{node: node_name, param: @default_vhost})
  end

  def usage, do: "clear_permissions [-p vhost] <username>"

  defp info(_, %{quiet: true}), do: nil
  defp info(username, %{param: vhost}), do: IO.puts "Clearing permissions for user \"#{username}\" in vhost \"#{vhost}\" ..."

  def flags, do: @flags
end
