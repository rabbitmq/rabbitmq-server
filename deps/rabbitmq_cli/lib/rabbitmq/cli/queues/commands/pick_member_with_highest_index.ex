## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.PickMemberWithHighestIndexCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.DataCoercion

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def switches(), do: [index: :string, timeout: :integer]
  def aliases(), do: [i: :index, t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", index: "log"}, opts)}
  end

  def run([name] = _args, %{vhost: vhost, index: index, node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :get_member_with_highest_index, [
           vhost,
           name,
           to_atom(String.downcase(index))
         ]) do
      {:error, :classic_queue_not_supported} ->
        index = format_index(String.downcase(index))
        {:error, "Cannot get #{index} index from a classic queue"}

      {:error, :not_found} ->
        {:error, {:not_found, :queue, vhost, name}}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage, do: "pick_member_with_highest_index <queue> [--vhost <vhost>] [--index <commit|commit_index|log|log_index|snapshot|snapshot_index>]"

  def usage_additional do
    [
      ["<queue>", "quorum queue name"],
      ["--index <commit|commit_index|log|log_index|snapshot|snapshot_index>", "name of the index to use to lookup highest member"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def help_section, do: :replication

  def description, do: "Look up the member of a quorum queue with the highest commit, log or snapshot index."

  def banner([name], %{node: node, index: index, vhost: vhost}) do
    index = format_index(String.downcase(index))
    "Member with highest #{index} index for queue #{name} in vhost #{vhost} on node #{node}..."
  end

  defp format_index("log_index"), do: "log"
  defp format_index("commit_index"), do: "commit"
  defp format_index("snapshot_index"), do: "snapshot"
  defp format_index(index_name), do: index_name
end
