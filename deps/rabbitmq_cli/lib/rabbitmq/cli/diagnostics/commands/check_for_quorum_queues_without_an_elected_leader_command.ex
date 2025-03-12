## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckForQuorumQueuesWithoutAnElectedLeaderCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  def switches(), do: [across_all_vhosts: :boolean]

  def scopes(), do: [:diagnostics]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{across_all_vhosts: false, vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([pattern] = _args, %{node: node_name, vhost: vhost, across_all_vhosts: across_all_vhosts_opt}) do
    vhost = if across_all_vhosts_opt, do: :across_all_vhosts, else: vhost

    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :leader_health_check, [pattern, vhost]) do
      [] ->
        :ok

      unhealthy_queues_or_error ->
        {:error, unhealthy_queues_or_error}
    end
  end

  def output(:ok, %{node: node_name, formatter: "json"}) do
    {:ok,
     %{
       "result" => "ok",
       "message" =>
         "Node #{node_name} reported all quorum queue as having responsive leader replicas"
     }}
  end

  def output(:ok, %{silent: true}) do
    {:ok, :check_passed}
  end

  def output(:ok, %{node: node_name}) do
    {:ok, "Node #{node_name} reported all quorum queue as having responsive leader replicas"}
  end

  def output({:error, unhealthy_queues}, %{node: node_name, formatter: "json"}) when is_list(unhealthy_queues) do
    {:ok, :check_passed,
     %{
       "result" => "error",
       "queues" => unhealthy_queues,
       "message" => "Node #{node_name} reported quorum queues with a missing (not elected) or unresponsive leader replica"
     }}
  end

  def output({:error, unhealthy_queues}, %{silent: true}) when is_list(unhealthy_queues) do
    {:ok, :check_passed}
  end

  def output({:error, unhealthy_queues}, %{vhost: _vhost}) when is_list(unhealthy_queues) do
    lines = queue_lines(unhealthy_queues)

    {:ok, :check_passed, Enum.join(lines, line_separator())}
  end

  def usage() do
    "check_for_quorum_queues_without_an_elected_leader [--vhost <vhost>] [--across-all-vhosts] <pattern>"
  end

  def usage_additional do
    [
      ["<pattern>", "regular expression pattern used to match quorum queues"],
      ["--across-all-vhosts", "run this health check across all existing virtual hosts"]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def usage_doc_guides() do
    [
      DocGuide.monitoring(),
      DocGuide.quorum_queues()
    ]
  end

  def description(), do: "Checks that quorum queue have elected and available leader replicas"

  def banner([name], %{across_all_vhosts: true}),
    do: "Checking leader replicas of quorum queues matching '#{name}' in all vhosts ..."

  def banner([name], %{vhost: vhost}),
    do: "Checking leader replicas of quorum queues matching '#{name}' in vhost #{vhost} ..."

  def queue_lines(qs) do
    for q <- qs, do: "#{q["readable_name"]} does not have an elected leader replica or the replica was unresponsive"
  end
end
