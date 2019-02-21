## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
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

defmodule RabbitMQ.CLI.Ctl.Commands.ShutdownCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  alias RabbitMQ.CLI.Core.OsPid

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

  def validate([], %{node: node_name, wait: true, timeout: timeout}) do
    hostname = :inet_db.gethostname()
    case :rabbit_misc.rpc_call(node_name, :inet_db, :gethostname, [], timeout) do
      {:badrpc, _} = err -> {:error, err}
      remote_hostname    ->
        case hostname == remote_hostname do
          true  -> :ok;
          false ->
            msg = "\nThis command can only --wait for shutdown of local nodes but node #{node_name} is remote (local hostname: #{hostname}, remote: #{remote_hostname}).\n" <>
                  "Pass --no-wait to shut node #{node_name} down without waiting.\n"
            {:validation_failure, {:unsupported_target, msg}}
        end
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

  def help_section(), do: :node_management

  def description(), do: "Stops the Erlang node on which RabbitMQ is running and waits for the OS process to exit"

  def banner(_, _), do: nil


  #
  # Implementation
  #

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
