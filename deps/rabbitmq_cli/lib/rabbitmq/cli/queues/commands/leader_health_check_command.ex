## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.LeaderHealthCheckCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  def scopes(), do: [:queues]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.MergesDefaultVirtualHost
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([pattern] = _args, %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :leader_health_check, [pattern, vhost]) do
      [] ->
        :ok

      unhealthy_queues ->
        {:error, unhealthy_queues}
    end
  end

  def output(:ok, %{node: node_name, formatter: "json"}) do
    {:ok,
     %{
       "result" => "ok",
       "message" =>
         "Node #{node_name} reported all quorum queue leaders as healthy"
     }}
  end

  def output(:ok, %{silent: true}) do
    {:ok, :check_passed}
  end

  def output(:ok, %{node: node_name}) do
    {:ok, "Node #{node_name} reported all quorum queue leaders as healthy"}
  end

  def output({:error, unhealthy_queues}, %{node: node_name, formatter: "json"}) when is_list(unhealthy_queues) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "queues" => unhealthy_queues,
       "message" => "Node #{node_name} reported unhealthy quorum queue leaders"
     }}
  end

  def output({:error, unhealthy_queues}, %{silent: true}) when is_list(unhealthy_queues) do
    {:error, :check_failed}
  end

  def output({:error, unhealthy_queues}, %{vhost: _vhost}) when is_list(unhealthy_queues) do
    lines = queue_lines(unhealthy_queues)

    {:error, :check_failed, Enum.join(lines, line_separator())}
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "leader_health_check [--vhost <vhost>] <pattern>"
  end

  def usage_additional do
    [
      ["<pattern>", "regular expression pattern used to match quorum queues"]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def description(), do: "Checks availability and health status of quorum queue leaders"

  def banner([name], %{vhost: vhost}),
    do: "Checking availability and health status of leaders for quorum queues matching #{name} in vhost #{vhost} ..."

  def queue_lines(qs) do
    for q <- qs, do: "Leader for #{q["readable_name"]} is unhealthy and unavailable"
  end
end
