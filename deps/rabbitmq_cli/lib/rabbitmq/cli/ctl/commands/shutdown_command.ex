## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ShutdownCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  alias RabbitMQ.CLI.Core.{OsPid, NodeName}

  def switches() do
    [timeout: :integer,
     wait: :boolean]
  end

  def aliases(), do: [timeout: :t]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{wait: true, timeout: 120}, opts)}
  end

  def validate([], %{wait: false}) do
    :ok
  end

  def validate([], %{node: node_name, wait: true}) do
    local_hostname = NodeName.hostname_from_node(Node.self())
    remote_hostname = NodeName.hostname_from_node(node_name)
    case addressing_local_node?(local_hostname, remote_hostname) do
      true  -> :ok;
      false ->
        msg = "\nThis command can only --wait for shutdown of local nodes " <>
              "but node #{node_name} seems to be remote " <>
              "(local hostname: #{local_hostname}, remote: #{remote_hostname}).\n" <>
              "Pass --no-wait to shut node #{node_name} down without waiting.\n"
        {:validation_failure, {:unsupported_target, msg}}
    end
  end
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, wait: false, timeout: timeout}) do
    shut_down_node_without_waiting(node_name, timeout)
  end

  def run([], %{node: node_name, wait: true, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :os, :getpid, []) do
      pid when is_list(pid) ->
        shut_down_node_and_wait_pid_to_stop(node_name, pid, timeout)
      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "shutdown [--wait]"

  def usage_additional() do
    [
      ["--wait", "if set, will wait for target node to terminate (by inferring and monitoring its PID file). Only works for local nodes."],
      ["--no-wait", "if set, will not wait for target node to terminate"]
    ]
  end

  def help_section(), do: :node_management

  def description(), do: "Stops RabbitMQ and its runtime (Erlang VM). Monitors progress for local nodes. Does not require a PID file path."

  def banner(_, _), do: nil


  #
  # Implementation
  #

  def addressing_local_node?(_, remote_hostname) when remote_hostname == :localhost , do: :true
  def addressing_local_node?(_, remote_hostname) when remote_hostname == 'localhost', do: :true
  def addressing_local_node?(_, remote_hostname) when remote_hostname == "localhost", do: :true
  def addressing_local_node?(local_hostname, remote_hostname) do
    local_hostname == remote_hostname
  end

  defp shut_down_node_without_waiting(node_name, timeout) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :stop_and_halt, [], timeout)
  end

  defp shut_down_node_and_wait_pid_to_stop(node_name, pid, timeout) do
    {:stream,
     RabbitMQ.CLI.Core.Helpers.stream_until_error([
       fn -> "Shutting down RabbitMQ node #{node_name} running at PID #{pid}" end,
       fn ->
         res = shut_down_node_without_waiting(node_name, timeout)

         case res do
           :ok -> "Waiting for PID #{pid} to terminate"
           {:badrpc, err} -> {:error, err}
           {:error, _} = err -> err
         end
       end,
       fn ->
         OsPid.wait_for_os_process_death(pid)
         "RabbitMQ node #{node_name} running at PID #{pid} successfully shut down"
       end
     ])}
  end
end
