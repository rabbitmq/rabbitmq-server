## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ListNodeAuthAttemptStatsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def switches(), do: [by_source: :boolean]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{by_source: false}, opts)}
  end

  def validate([], _), do: :ok
  def validate(_, _), do: {:validation_failure, :too_many_args}

  def run([], %{node: node_name, timeout: timeout, by_source: by_source}) do
    case by_source do
      :true ->
        :rabbit_misc.rpc_call(
          node_name, :rabbit_core_metrics, :get_auth_attempts_by_source, [], timeout)
      :false ->
        :rabbit_misc.rpc_call(
          node_name, :rabbit_core_metrics, :get_auth_attempts, [], timeout)
    end
  end

  def output([], %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "node" => node_name, "attempts" => []}}
  end
  def output([], %{node: node_name})  do
    {:ok, "Node #{node_name} reported no authentication attempt stats"}
  end
  def output(rows, %{node: node_name, formatter: "json"}) do
    maps = Enum.map(rows, &Map.new/1)
    {:ok,
     %{
       "result"   => "ok",
       "node"     => node_name,
       "attempts" => maps
     }}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "list_node_auth_attempts [--by-source]"

  def usage_additional do
    [
      ["--by-source", "list authentication attempts by remote address and username"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :observability_and_health_checks
  def description(), do: "Lists authentication attempts on the target node"

  def banner([], %{node: node_name}), do: "Listing authentication
 attempts for node \"#{node_name}\" ..."
end
