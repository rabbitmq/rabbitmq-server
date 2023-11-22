## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.RemoteShellCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  @dialyzer :no_missing_calls
  @dialyzer {:nowarn_function, [run: 2, start_shell_on_otp_26_plus: 1, start_shell_on_otp_25: 1]}

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name}) do
    _ = :c.l(:shell)

    if :erlang.function_exported(:shell, :start_interactive, 1) do
      start_shell_on_otp_26_plus(node_name)
    else
      start_shell_on_otp_25(node_name)
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Starts an interactive Erlang shell on the target node"

  def usage, do: "remote_shell"

  def banner(_, %{node: node_name}) do
    "Starting an interactive Erlang shell on node #{node_name}... Press 'Ctrl+G' then 'q' to exit."
  end

  defp start_shell_on_otp_26_plus(node_name) do
    case :shell.start_interactive({node_name, {:shell, :start, []}}) do
      :ok -> :ok
      {:error, :already_started} -> :ok
      {error, _} -> {:error, {:badrpc, :nodedown}}
    end

    :timer.sleep(:infinity)
  end

  defp start_shell_on_otp_25(node_name) do
    _ = Supervisor.terminate_child(:kernel_sup, :user)
    Process.flag(:trap_exit, true)
    user_drv = :user_drv.start([~c"tty_sl -c -e", {node_name, :shell, :start, []}])
    Process.link(user_drv)

    receive do
      {~c"EXIT", _user_drv, _} ->
        {:ok, "Disconnected from #{node_name}."}
    end
  end
end
