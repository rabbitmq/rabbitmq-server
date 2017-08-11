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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.ListVhostLimitsCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def switches(), do: [global: :boolean]

  def merge_defaults(args, %{global: true} = opts) do
    {args, opts}
  end
  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate([_|_], _) do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, global: true}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_vhost_limit, :list, []) do
      []              -> []
      {:error, err}   -> {:error, err}
      {:badrpc, node} -> {:badrpc, node}
      val             ->
        Enum.map(val, fn({vhost, val}) ->
          {:ok, val_encoded} = JSON.encode(Map.new(val))
          [vhost: vhost, limits: val_encoded]
        end)
    end
  end

  def run([], %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_vhost_limit, :list, [vhost]) do
      []              -> []
      {:error, err}   -> {:error, err}
      {:badrpc, node} -> {:badrpc, node}
      val when is_list(val) or is_map(val)  ->
        JSON.encode(Map.new(val))
    end
  end

  def usage, do: "list_vhost_limits [-p <vhost>] [--global]"

  def banner([], %{global: true}) do
    "Listing limits for all vhosts ..."
  end
  def banner([], %{vhost: vhost}) do
    "Listing limits for vhost \"#{vhost}\" ..."
  end
end
