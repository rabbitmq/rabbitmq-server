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

  @default_vhost "/"

  def clear_permissions([], _) do
    {:not_enough_args, []}
  end

  def clear_permissions([_|_] = args, _) when length(args) > 1 do
    {:too_many_args, args}
  end

  def clear_permissions([username], %{node: node_name, param: vhost}) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_auth_backend_internal, :clear_permissions, [username, vhost])
  end

  def clear_permissions([username], %{node: node_name}) do
    clear_permissions([username], %{node: node_name, param: @default_vhost})
  end

  def usage, do: "clear_permissions [-p vhost] <username>"
end
