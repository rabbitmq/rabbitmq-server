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

defmodule RabbitMQ.CLI.Ctl.Commands.StatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.{Listeners, Platform}

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
    :rabbit_misc.rpc_call(node_name, :rabbit, :status, [], timeout)
  end

  def output({:error, :timeout}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: timed out while waiting for a response from #{node_name}."}
  end
  def output(result, _opts) when is_list(result) do
    m = %{
      os: os_name(Keyword.get(result, :os)),
      pid: Keyword.get(result, :pid),
      erlang_version: Keyword.get(result, :erlang_version) |> to_string |> String.trim_trailing,
      uptime: Keyword.get(result, :uptime),
      processes: Enum.into(Keyword.get(result, :processes), %{}),
      run_queue: Keyword.get(result, :run_queue),
      net_ticktime: net_ticktime(result),
      vm_memory_calculation_strategy: Keyword.get(result, :vm_memory_calculation_strategy),
      vm_memory_high_watermark: Keyword.get(result, :vm_memory_high_watermark),
      vm_memory_limit: Keyword.get(result, :vm_memory_limit),
      disk_free_limit: Keyword.get(result, :disk_free_limit),
      disk_free: Keyword.get(result, :disk_free),
      file_descriptors: Enum.into(Keyword.get(result, :file_descriptors), %{}),
      alarms: Keyword.get(result, :alarms),
      listeners: listener_maps(Keyword.get(result, :listeners, [])),
      memory: Keyword.get(result, :memory) |> Keyword.update(:total, [], fn x -> Enum.into(x, %{}) end) |> Enum.into(%{})
    }

    {:ok, m}
  end
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "status"

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays broker status information"

  def banner(_, %{node: node_name}), do: "Status of node #{node_name} ..."

  #
  # Implementation
  #

  defp net_ticktime(result) do
    case Keyword.get(result, :kernel) do
      {:net_ticktime, n}   -> n
      n when is_integer(n) -> n
      _                    -> :undefined
    end
  end
end
