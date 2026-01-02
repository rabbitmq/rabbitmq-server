## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.ShrinkCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches() do
    [errors_only: :boolean]
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{errors_only: false}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([node], %{node: node_name, errors_only: errs}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :delete_all_replicas, [
           to_atom(node)
         ]) do
      {:error, _} = error ->
        error

      {:badrpc, _} = error ->
        error

      results when errs ->
        for {{:resource, vhost, _kind, name}, {:error, _, _} = res} <- results,
            do: [
              {:vhost, vhost},
              {:name, name},
              {:size, format_size(res)},
              {:result, format_result(res)}
            ]

      results ->
        for {{:resource, vhost, _kind, name}, res} <- results,
            do: [
              {:vhost, vhost},
              {:name, name},
              {:size, format_size(res)},
              {:result, format_result(res)}
            ]
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def banner([node], _) do
    "Shrinking streams on #{node}..."
  end

  def usage, do: "shrink <node> [--errors-only]"

  def usage_additional() do
    [
      ["<node>", "node name to remove replicas from"],
      ["--errors-only", "only list streams which reported an error"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section, do: :cluster_management

  def description,
    do: "Shrinks stream clusters by removing any replicas on the given node."

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

  defp format_result({:error, _size, {:error, :last_stream_member}}) do
    "error: the last replica cannot be removed"
  end

  defp format_result({:error, _size, :timeout}) do
    "error: the operation timed out and may not have been completed"
  end

  defp format_result({:error, _size, err}) do
    to_string(:io_lib.format("error: ~W", [err, 10]))
  end
end
