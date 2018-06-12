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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2018 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.PingCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 60_000

  def scopes(), do: [:ctl, :diagnostics]

  def merge_defaults(args, opts) do
    timeout = case opts[:timeout] do
      nil       -> @default_timeout;
      :infinity -> @default_timeout;
      other     -> other
    end
    {args, Map.merge(opts, %{timeout: timeout})}
  end

  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def validate([_], _), do: {:validation_failure, :too_many_args}
  def validate([], _), do: :ok

  def run([], %{node: node_name, timeout: timeout}) do
    # this is very similar to what net_adm:ping/1 does reimplemented with support for custom timeouts
    # and error values that are used by CLI commands
    msg = "Failed to connect and authenticate to #{node_name} in #{timeout} ms"
    try do
      case :gen.call({:net_kernel, node_name}, :'$gen_call', {:is_auth, node()}, timeout) do
        :ok      -> :ok
        {:ok, _} -> :ok
        _        ->
          :erlang.disconnect_node(node_name)
          {:error, msg}
      end
    catch :exit, _ ->
            :erlang.disconnect_node(node_name)
            {:error, msg}
          _ ->
            :erlang.disconnect_node(node_name)
            {:error, msg}
    end
  end

  def usage() do
    "ping"
  end

  def banner([], %{node: node_name, timeout: timeout}) when is_number(timeout) do
    "Will ping #{node_name}. This only checks if the OS process is running and registered with epmd. Timeout: #{timeout} ms."
  end
  def banner([], %{node: node_name, timeout: _timeout}) do
    "Will ping #{node_name}. This only checks if the OS process is running and registered with epmd."
  end

  def output(:ok, _) do
    {:ok, "Ping succeeded"}
  end
  def output({:error, :timeout}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software,
     "Error: timed out while waiting for a response from #{node_name}."}
  end
  use RabbitMQ.CLI.DefaultOutput
end
