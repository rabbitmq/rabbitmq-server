## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.TlsVersionsCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout} = _opts) do
    :rabbit_misc.rpc_call(node_name, :ssl, :versions, [], timeout)
  end

  def banner([], %{}), do: "Listing all TLS versions supported by the runtime..."

  def output(result, %{formatter: "json"}) do
    vs = Map.new(result) |> Map.get(:available)

    {:ok, %{versions: vs}}
  end

  def output(result, _opts) do
    vs = Map.new(result) |> Map.get(:available)
    {:ok, vs}
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists TLS versions supported (but not necessarily allowed) on the target node"

  def usage, do: "tls_versions"

  def formatter(), do: RabbitMQ.CLI.Formatters.StringPerLine
end
