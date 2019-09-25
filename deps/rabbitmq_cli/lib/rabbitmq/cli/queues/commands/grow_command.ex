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

defmodule RabbitMQ.CLI.Queues.Commands.GrowCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import Rabbitmq.Atom.Coerce

  @behaviour RabbitMQ.CLI.CommandBehaviour

  defp default_opts, do: %{vhost_pattern: ".*",
                           queue_pattern: ".*",
                           errors_only: false}

  def switches(),
    do: [
    vhost_pattern: :string,
    queue_pattern: :string,
    errors_only: :boolean
  ]

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end
  def validate([_, s], _) do
   case s do
     "all" -> :ok
     "even" -> :ok
     _ ->
       {:validation_failure, "strategy '#{s}' is not recognised."}
   end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([node, strategy], %{node: node_name,
                              vhost_pattern: vhost_pat,
                              queue_pattern: queue_pat,
                              errors_only: errors_only}) do
        case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :grow, [
          to_atom(node),
          vhost_pat,
          queue_pat,
          to_atom(strategy)]) do
      {:error, _}  = error -> error;
      {:badrpc, _} = error -> error;
      results when errors_only ->
        for {{:resource, vhost, _kind, name}, {:errors, _, _} = res} <- results,
        do: [{:vhost, vhost},
             {:name, name},
             {:size, format_size res},
             {:result, format_result res}]
      results ->
        for {{:resource, vhost, _kind, name}, res} <- results,
        do: [{:vhost, vhost},
             {:name, name},
             {:size, format_size res},
             {:result, format_result res}]
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "grow <node> <all | even> [--vhost-pattern <pattern>] [--queue-pattern <pattern>]"

  def usage_additional do
    [
      ["<node>", "node name to place replicas on"],
      ["<all | even>", "how many matching quorum queues should have a replica added on this node: all or half (evenly numbered)?"],
      ["--queue-pattern <pattern>", "regular expression to match queue names"],
      ["--vhost-pattern <pattern>", "regular expression to match virtual host names"],
      ["--errors-only", "only list queues which reported an error"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :cluster_management

  def description, do: "Grows quorum queue clusters by adding a member (replica) to all or half of matching quorum queues on the given node."

  def banner([node, strategy], _) do
    "Growing #{strategy} quorum queues on #{node}..."
  end

  #
  # Implementation
  #

  defp format_size({:ok, size}) do
    size
  end
  defp format_size({:error, _size, :timeout}) do
    # the actual size is uncertain here
    "?"
  end
  defp format_size({:error, size, _}) do
    size
  end

  defp format_result({:ok, _size}) do
    "ok"
  end
  defp format_result({:error, _size, :timeout}) do
    "error: the operation timed out and may not have been completed"
  end
  defp format_result({:error, _size, err}) do
    :io.format "error: ~W", [err, 10]
  end
end
