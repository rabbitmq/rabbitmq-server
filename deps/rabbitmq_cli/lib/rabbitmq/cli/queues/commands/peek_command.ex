## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.PeekCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:queues]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  use RabbitMQ.CLI.Core.MergesDefaultVirtualHost

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end
  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end
  def validate([_, raw_pos], _) do
    pos = case Integer.parse(raw_pos) do
      {n, _} -> n
      :error -> :error
      _      -> :error
    end

    invalid_pos = {:validation_failure, "position value must be a positive integer"}
    case pos do
      :error            -> invalid_pos
      num when num < 1  -> invalid_pos
      num when num >= 1 -> :ok
    end
  end
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, pos] = _args, %{node: node_name, vhost: vhost}) do
    {pos, _} = Integer.parse(pos)
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :peek, [vhost, name, pos]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot peek into a classic queue"}
      {:ok, msg} ->
        {:ok, msg}
      err ->
        err
    end
  end

  def output({:error, :not_found}, %{vhost: vhost, formatter: "json"}) do
    {:error,
     %{
       "result" => "error",
       "message" => "Target queue was not found in virtual host '#{vhost}'"
     }}
  end
  def output({:error, :no_message_at_pos}, %{formatter: "json"}) do
    {:error,
     %{
       "result" => "error",
       "message" => "Target queue does not have a message at that position"
     }}
  end
  def output({:error, error}, %{formatter: "json"}) do
    {:error,
     %{
       "result" => "error",
       "message" => "Failed to perform the operation: #{error}"
     }}
  end
  def output({:error, :not_found}, %{vhost: vhost}) do
    {:error, "Target queue was not found in virtual host '#{vhost}'"}
  end
  def output({:error, :no_message_at_pos}, _) do
    {:error, "Target queue does not have a message at that position"}
  end
  def output({:ok, msg}, %{formatter: "json"}) do
    {:ok, %{"result" => "ok", "message" => Enum.into(msg, %{})}}
  end
  def output({:ok, msg}, _) do
    res = Enum.map(msg, fn {k,v} -> [{"keys", k}, {"values", v}] end)
    {:stream, res}
  end
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "peek [--vhost <vhost>] <queue> <position>"
  end

  def usage_additional do
    [
      ["<queue>", "Name of the queue",
       "<position>", "Position in the queue, starts at 1"]
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def usage_doc_guides() do
    [
      DocGuide.quorum_queues()
    ]
  end

  def description(), do: "Peeks at the given position of a quorum queue"

  def banner([name, pos], %{node: node_name}),
    do: "Peeking at quorum queue #{name} at position #{pos} on node #{node_name} ..."
end
