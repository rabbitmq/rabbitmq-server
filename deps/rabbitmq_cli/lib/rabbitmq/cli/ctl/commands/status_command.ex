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
  import RabbitMQ.CLI.Core.{Alarms, Listeners, Memory, Platform}

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
  def output(result, %{formatter: "json"}) when is_list(result) do
    m = result_map(result) |> Map.update(:alarms, [], fn xs -> alarm_maps(xs) end)

    {:ok, m}
  end
  def output(result, %{node: node_name}) when is_list(result) do
    m = result_map(result)

    runtime_section = [
      "#{bright("Runtime")}\n",
      "OS PID: #{m[:pid]}",
      "OS: #{m[:os]}",
      # TODO: format
      "Uptime (seconds): #{m[:uptime]}",
      "Node name: #{node_name}",
      "Erlang configuration: #{m[:erlang_version]}",
      "Erlang processes: #{m[:processes][:used]} used, #{m[:processes][:limit]} limit",
      "Scheduler run queue: #{m[:run_queue]}",
      "Cluster heartbeat timeout (net_ticktime): #{m[:net_ticktime]}"
    ]

    plugin_section = [
      "\n#{bright("Plugins")}\n",
      "Enabled plugin file: #{m[:enabled_plugin_file]}",
      "Enabled plugins:\n"
    ] ++ Enum.map(m[:active_plugins], fn pl -> " * #{pl}" end)

    config_section = [
      "\n#{bright("Config files")}\n"
    ] ++ Enum.map(m[:config_files], fn path -> " * #{path}" end)

    log_section = [
      "\n#{bright("Log file(s)")}\n"
    ] ++ Enum.map(m[:log_files], fn path -> " * #{path}" end)

    alarms_section = [
      "\n#{bright("Alarms")}\n",
    ] ++ case m[:alarms] do
           [] -> ["(none)"]
           xs -> alarm_lines(xs, node_name)
         end

    breakdown = compute_relative_values(m[:memory])
    memory_section = [
      "\n#{bright("Memory")}\n",
      "Calculation strategy: #{m[:vm_memory_calculation_strategy]}",
      "Memory high watermark: #{m[:vm_memory_high_watermark]}, limit in bytes: #{m[:vm_memory_limit]}"
    ] ++ Enum.map(breakdown, fn({category, val}) -> "#{category}: #{val[:bytes]} bytes (#{val[:percentage]} %)" end)

    file_descriptors = [
      "\n#{bright("File Descriptors")}\n",
      "Total: #{m[:file_descriptors][:total_used]}, limit: #{m[:file_descriptors][:total_limit]}",
      "Sockets: #{m[:file_descriptors][:sockets_used]}, limit: #{m[:file_descriptors][:sockets_limit]}"
    ]

    disk_space_section = [
      "\n#{bright("Free Disk Space")}\n",
      "Low free disk space watermark: #{m[:disk_free_limit]}",
      # TODO: format
      "Free disk space: #{m[:disk_free]}"
    ]
    listeners_section = [
      "\n#{bright("Listeners")}\n",
    ] ++ case m[:listeners] do
           [] -> ["(none)"]
           xs -> listener_lines(xs)
         end
    lines = runtime_section ++ plugin_section ++ config_section ++ log_section ++
            alarms_section ++ memory_section ++ file_descriptors ++
            disk_space_section ++ listeners_section

    {:ok, Enum.join(lines, line_separator())}
  end
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.String

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

  defp result_map(result) do
    %{
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
      memory: Keyword.get(result, :memory) |> Enum.into(%{}),

      config_files: Keyword.get(result, :config_files) |> Enum.map(&to_string/1),
      log_files: Keyword.get(result, :log_files) |> Enum.map(&to_string/1),

      active_plugins: Keyword.get(result, :active_plugins) |> Enum.map(&to_string/1),
      enabled_plugin_file: Keyword.get(result, :enabled_plugin_file) |> to_string
    }
  end

  defp net_ticktime(result) do
    case Keyword.get(result, :kernel) do
      {:net_ticktime, n}   -> n
      n when is_integer(n) -> n
      _                    -> :undefined
    end
  end

  defp bright(string) do
    "#{IO.ANSI.bright()}#{string}#{IO.ANSI.reset()}"
  end
end
