## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListExchangesCommand do
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @info_keys ~w(name type durable auto_delete internal arguments policy)a

  def info_keys(), do: @info_keys

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults([], opts) do
    merge_defaults(~w(name type), opts)
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
      :rabbit_exchange,
      :info_all,
      [vhost, info_keys],
      timeout,
      info_keys
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage(), do: "list_exchanges [--vhost <vhost>] [--no-table-headers] [<column> ...]"

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists exchanges"

  def banner(_, %{vhost: vhost}), do: "Listing exchanges for vhost #{vhost} ..."
end
