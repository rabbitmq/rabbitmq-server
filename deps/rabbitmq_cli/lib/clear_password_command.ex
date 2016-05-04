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


defmodule ClearPasswordCommand do

  @behaviour CommandBehaviour
  @flags []

  def clear_password([], _), do: {:not_enough_args, []}
  def clear_password([_|_] = args, _) when length(args) > 1, do: {:too_many_args, args}
  def clear_password([_user] = args, %{node: node_name} = opts) do
    info(args, opts)
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_auth_backend_internal, :clear_password, args)
  end

  def usage, do: "clear_password <username>"

  defp info(_, %{quiet: true}), do: nil
  defp info([user], _), do: IO.puts "Clearing password for user \"#{user}\" ..."

  def flags, do: @flags
end
