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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Upgrade.Commands.AwaitOnlineQuorumPlusOneCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 120

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        val -> val
      end

    {args, Map.put(opts, :timeout, timeout)}
  end


  def run([], %{node: node_name, timeout: timeout}) do
    rpc_timeout = timeout + 500
    case :rabbit_misc.rpc_call(node_name, :rabbit_upgrade_preparation, :await_online_quorum_plus_one, [timeout], rpc_timeout) do
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      {:badrpc, _} = err -> err

      true  -> :ok
      false -> {:error, "time is up, no quorum + 1 online replicas came online for at least some quorum queues"}
    end
  end

  def output({:error, msg}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => msg}}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "await_online_quorum_plus_one"

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues(),
      DocGuide.upgrade()
    ]
  end

  def help_section, do: :upgrade

  def description() do
    "Waits for all quorum queues to have an above minimum online quorum. " <>
    "This makes sure that no queues would lose their quorum if the target node is shut down"
  end

  def banner([], %{timeout: timeout}) do
    "Will wait for a quorum + 1 of nodes to be online for all quorum queues for #{round(timeout/1000)} seconds..."
  end
end
