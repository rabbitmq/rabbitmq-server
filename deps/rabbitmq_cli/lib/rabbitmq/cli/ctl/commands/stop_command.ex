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

defmodule RabbitMQ.CLI.Ctl.Commands.StopCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  alias RabbitMQ.CLI.Core.OsPid

  def merge_defaults(args, opts) do
    {args, Map.merge(%{idempotent: false}, opts)}
  end

  def switches(), do: [idempotent: :boolean]

  def validate([], _), do: :ok
  def validate([_pidfile_path], _), do: :ok
  def validate([_ | _] = args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}

  def run([], %{node: node_name, idempotent: true}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit, :stop_and_halt, []) do
      {:badrpc, :nodedown} -> {:ok, "Node #{node_name} is no longer running"}
      any -> any
    end
  end

  def run([], %{node: node_name, idempotent: false}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :stop_and_halt, [])
  end

  def run([pidfile_path], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :stop_and_halt, [])

    case OsPid.read_pid_from_file(pidfile_path, true) do
      {:error, details} ->
        {:error, "could not read pid from file #{pidfile_path}. Error: #{details}"}

      {:error, :could_not_read_pid_from_file, {:contents, s}} ->
        {:error, "could not read pid from file #{pidfile_path}. File contents: #{s}"}

      {:error, :could_not_read_pid_from_file, details} ->
        {:error, "could not read pid from file #{pidfile_path}. Error: #{details}"}

      pid ->
        OsPid.wait_for_os_process_death(pid)
        {:ok, "process #{pid} (take from pid file #{pidfile_path}) is no longer running"}
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "stop [--idempotent] [<pidfile>]"

  def usage_additional() do
    [
      ["<pidfile>", "node PID file path to monitor. To avoid using a PID file, use 'rabbitmqctl shutdown'"],
      ["--idempotent", "return success if target node is not running (cannot be contacted)"]
    ]
  end

  def description(), do: "Stops RabbitMQ and its runtime (Erlang VM). Requires a local node pid file path to monitor progress."

  def help_section(), do: :node_management

  def banner([pidfile_path], %{node: node_name}) do
    "Stopping and halting node #{node_name} (will monitor pid file #{pidfile_path}) ..."
  end

  def banner(_, %{node: node_name}), do: "Stopping and halting node #{node_name} ..."
end
