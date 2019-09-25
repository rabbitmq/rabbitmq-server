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

defmodule RabbitMQ.CLI.Queues.Commands.QuorumStatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:ctl, :diagnostics, :queues]

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = _args, %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :status, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot get quorum status of a classic queue"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "quorum_status [--vhost <vhost>] <queue>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the queue"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section(), do: :queues

  def description(), do: "Displays quorum status of a quorum queue"

  def banner([name], %{node: node_name}),
    do: "Status of quorum queue #{name} on node #{node_name} ..."
end
