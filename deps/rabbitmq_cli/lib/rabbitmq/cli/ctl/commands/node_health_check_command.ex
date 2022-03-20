## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
     "Error: health check failed. Message: #{message}"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "node_health_check"

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :deprecated
  def description() do
    "DEPRECATED. Performs intrusive, opinionated health checks on a fully booted node. " <>
    "See https://www.rabbitmq.com/monitoring.html#health-checks instead"
  end

  def banner(_, %{node: node_name, timeout: timeout}) do
    [
      "This command is DEPRECATED and will be removed in a future version.",
      "It performs intrusive, opinionated health checks and requires a fully booted node.",
      "Use one of the options covered in https://www.rabbitmq.com/monitoring.html#health-checks instead.",
      "Timeout: #{trunc(timeout / 1000)} seconds ...",
      "Checking health of node #{node_name} ..."
    ]
  end
end
