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
