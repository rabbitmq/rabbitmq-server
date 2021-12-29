## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
    ret = OsPid.read_pid_from_file(pidfile_path, true)
    :rabbit_misc.rpc_call(node_name, :rabbit, :stop_and_halt, [])

    case ret do
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
