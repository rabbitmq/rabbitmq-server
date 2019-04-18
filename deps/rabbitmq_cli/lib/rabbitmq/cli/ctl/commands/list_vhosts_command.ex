## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListVhostsCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  alias RabbitMQ.CLI.Ctl.InfoKeys

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  @info_keys ~w(name tracing cluster_state)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults([], opts) do
    merge_defaults(["name"], opts)
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_ | _] = args, %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :info_all, [], timeout)
    |> filter_by_arg(args)
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "list_vhosts [--no-table-headers] [<column> ...]"

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts(),
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :access_control

  def description(), do: "Lists virtual hosts"

  def banner(_, _), do: "Listing vhosts ..."

  #
  # Implementation
  #

  defp filter_by_arg(vhosts, _) when is_tuple(vhosts) do
    vhosts
  end

  defp filter_by_arg(vhosts, [_ | _] = args) do
    symbol_args = InfoKeys.prepare_info_keys(args)

    vhosts
    |> Enum.map(fn vhost ->
      symbol_args
      |> Enum.filter(fn arg -> vhost[arg] != nil end)
      |> Enum.map(fn arg -> {arg, vhost[arg]} end)
    end)
  end
end
