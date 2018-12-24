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
## Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.TlsVersionsCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, opts}
  end

  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def usage, do: "tls_versions"

  def run([], %{node: node_name, timeout: timeout} = _opts) do
    :rabbit_misc.rpc_call(node_name, :ssl, :versions, [], timeout)
  end

  def banner([], %{}),  do: "Listing all TLS versions supported by the runtime..."

  def output(result, %{formatter: "json"}) do
    vs = Map.new(result) |> Map.get(:available)

    {:ok, %{versions: vs}}
  end
  def output(result, _opts) do
    vs = Map.new(result) |> Map.get(:available)
    {:ok, vs}
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.StringPerLine
end
