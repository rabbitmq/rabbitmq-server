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

defmodule RabbitMQ.CLI.Ctl.Commands.ListBindingsCommand do
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @info_keys ~w(source_name source_kind destination_name destination_kind routing_key arguments)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults([], opts) do
    merge_defaults(
      ~w(source_name source_kind
        destination_name destination_kind
        routing_key arguments),
      opts
    )
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", table_headers: true}, opts)}
  end

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_ | _] = args, %{node: node_name, timeout: timeout, vhost: vhost}) do
    info_keys = InfoKeys.prepare_info_keys(args)

    RpcStream.receive_list_items(
      node_name,
      :rabbit_binding,
      :info_all,
      [vhost, info_keys],
      timeout,
      info_keys
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage() do
    "list_bindings [--vhost <vhost>] [--no-table-headers] [<column> ...]"
  end

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists all bindings on a vhost"

  def banner(_, %{vhost: vhost}), do: "Listing bindings for vhost #{vhost}..."
end
