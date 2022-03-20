## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.PingCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 60_000

  def scopes(), do: [:ctl, :diagnostics]

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(opts, %{timeout: timeout})}
  end

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    # this is very similar to what net_adm:ping/1 does reimplemented with support for custom timeouts
    # and error values that are used by CLI commands
    msg = "Failed to connect and authenticate to #{node_name} in #{timeout} ms"

    try do
      case :gen.call({:net_kernel, node_name}, :"$gen_call", {:is_auth, node()}, timeout) do
        :ok ->
          :ok

        {:ok, _} ->
          :ok

        _ ->
          :erlang.disconnect_node(node_name)
          {:error, msg}
      end
    catch
      :exit, _ ->
        :erlang.disconnect_node(node_name)
        {:error, msg}

      _ ->
        :erlang.disconnect_node(node_name)
        {:error, msg}
    end
  end

  def output(:ok, _) do
    {:ok, "Ping succeeded"}
  end

  def output({:error, :timeout}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: timed out while waiting for a response from #{node_name}."}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "ping"
  end

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Checks that the node OS process is up, registered with EPMD and CLI tools can authenticate with it"

  def banner([], %{node: node_name, timeout: timeout}) when is_number(timeout) do
    "Will ping #{node_name}. This only checks if the OS process is running and registered with epmd. Timeout: #{
      timeout
    } ms."
  end

  def banner([], %{node: node_name, timeout: _timeout}) do
    "Will ping #{node_name}. This only checks if the OS process is running and registered with epmd."
  end
end
