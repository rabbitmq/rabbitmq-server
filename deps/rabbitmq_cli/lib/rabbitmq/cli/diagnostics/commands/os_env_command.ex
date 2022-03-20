## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.OsEnvCommand do
  @moduledoc """
  Lists RabbitMQ-specific environment variables defined on target node
  """

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_env, :get_used_env_vars, [], timeout) do
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      xs when is_list(xs) ->
        # convert keys and values to binaries (Elixir strings)
        xs
        |> Enum.map(fn {k, v} -> {:rabbit_data_coercion.to_binary(k), :rabbit_data_coercion.to_binary(v)} end)
        |> :maps.from_list
      other -> other
    end
  end

  def output([], %{formatter: fmt}) when fmt == "csv" or fmt == "erlang" do
    {:ok, []}
  end
  def output([], %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "variables" => []}}
  end
  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no relevant environment variables."}
  end
  def output(vars, %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "variables" => vars}}
  end
  def output(vars, %{formatter: "csv"}) do
    {:stream, [Enum.map(vars, fn({k, v}) -> [variable: k, value: v] end)]}
  end
  def output(vars, _opts) do
    lines = Enum.map(vars, fn({k, v}) -> "#{k}=#{v}" end) |> Enum.join(line_separator())
    {:ok, lines}
  end

  def usage() do
    "os_env"
  end

  def help_section(), do: :configuration

  def description(), do: "Lists RabbitMQ-specific environment variables set on target node"

  def banner(_, %{node: node_name}) do
    "Listing RabbitMQ-specific environment variables defined on node #{node_name}..."
  end
end
