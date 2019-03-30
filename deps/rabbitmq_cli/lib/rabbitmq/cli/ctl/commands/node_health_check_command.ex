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

defmodule RabbitMQ.CLI.Ctl.Commands.NodeHealthCheckCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 70_000

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(opts, %{timeout: timeout})}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_health_check, :node, [node_name, timeout]) do
      :ok ->
        :ok

      true ->
        :ok

      {:badrpc, _} = err ->
        err

      {:error_string, error_message} ->
        {:healthcheck_failed, error_message}

      {:node_is_ko, error_message, _exit_code} ->
        {:healthcheck_failed, error_message}

      other ->
        other
    end
  end

  def output(:ok, _) do
    {:ok, "Health check passed"}
  end

  def output({:healthcheck_failed, message}, _) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: healthcheck failed. Message: #{message}"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "node_health_check"

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :observability_and_health_checks
  def description(), do: "Performs several opinionated health checks of the target node"

  def banner(_, %{node: node_name, timeout: timeout}) do
    ["Timeout: #{trunc(timeout / 1000)} seconds ...", "Checking health of node #{node_name} ..."]
  end
end
