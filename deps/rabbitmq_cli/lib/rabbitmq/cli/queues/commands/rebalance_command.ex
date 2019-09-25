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

defmodule RabbitMQ.CLI.Queues.Commands.RebalanceCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  @known_types [
    "all",
    "classic",
    "quorum"
  ]

  defp default_opts, do: %{vhost_pattern: ".*",
                           queue_pattern: ".*"}

  def switches(),
    do: [
    vhost_pattern: :string,
    queue_pattern: :string
  ]

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end
  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([type], _) do
    case Enum.member?(@known_types, type) do
      true ->
        :ok

      false ->
        {:error, "type #{type} is not supported. Try one of all, classic, quorum."}
    end
  end

  def run([type], %{node: node_name,
                vhost_pattern: vhost_pat,
                queue_pattern: queue_pat}) do
    arg = String.to_atom(type)
    :rabbit_misc.rpc_call(node_name, :rabbit_amqqueue, :rebalance, [arg, vhost_pat, queue_pat])
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage, do: "rebalance < all | classic | quorum > [--vhost-pattern <pattern>] [--queue-pattern <pattern>]"

  def usage_additional do
    [
      ["<type>", "queue type, must be one of: all, classic, quorum"],
      ["--queue-pattern <pattern>", "regular expression to match queue names"],
      ["--vhost-pattern <pattern>", "regular expression to match virtual host names"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :cluster_management

  def description, do: "Rebalances queues."

  def banner([type], _) do
    "Rebalancing #{type} queues..."
  end

end
