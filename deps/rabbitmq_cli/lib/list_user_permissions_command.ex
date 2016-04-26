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


defmodule ListUserPermissionsCommand do

  def list_user_permissions([], _) do
    HelpCommand.help
    {:not_enough_args, []}
  end

  def list_user_permissions([_|_] = args, _) when length(args) > 1 do
    HelpCommand.help
    {:too_many_args, args}
  end

  def list_user_permissions([username], %{node: node_name, timeout: time_out}) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(
        :rabbit_auth_backend_internal,
        :list_user_permissions,
        [username],
        time_out
      )
  end

  def usage, do: "list_user_permissions <username>"
end
