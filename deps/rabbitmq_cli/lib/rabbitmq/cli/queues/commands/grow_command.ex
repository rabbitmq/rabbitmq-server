## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.GrowCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

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
      ["<all | even>", "add a member for all matching queues or just those whose membership count is an even number"],
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

  def description, do: "Grows quorum queue clusters by adding a member (replica) on the specified node for all matching queues"

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
