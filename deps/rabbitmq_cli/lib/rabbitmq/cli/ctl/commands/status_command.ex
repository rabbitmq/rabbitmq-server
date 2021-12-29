## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.StatusCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  alias RabbitMQ.CLI.InformationUnit, as: IU
  import RabbitMQ.CLI.Core.{Alarms, ANSI, DataCoercion, Listeners, Memory, Platform}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 60_000

  def scopes(), do: [:ctl, :diagnostics]

  def switches(), do: [unit: :string, timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(%{unit: "gb", timeout: timeout}, opts)}
  end

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, %{unit: unit}) do
    case IU.known_unit?(unit) do
      true ->
        :ok

      false ->
        {:validation_failure, "unit '#{unit}' is not supported. Please use one of: bytes, mb, gb"}
    end
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :status, [], timeout)
  end

  def output({:error, :timeout}, %{node: node_name}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Error: timed out while waiting for a response from #{node_name}."}
  end

  def output(result, %{formatter: "erlang"}) do
    {:ok, result}
  end

  def output(result, %{formatter: "json"}) when is_list(result) do
    m = result_map(result) |> Map.update(:alarms, [], fn xs -> alarm_maps(xs) end)

    {:ok, m}
  end

  def output(result, %{node: node_name, unit: unit}) when is_list(result) do
    m = result_map(result)

    product_name_section = case m do
      %{:product_name => product_name} when product_name != "" ->
        ["Product name: #{product_name}"]
      _ ->
        []
    end
    product_version_section = case m do
      %{:product_version => product_version} when product_version != "" ->
        ["Product version: #{product_version}"]
      _ ->
        []
    end

    runtime_section = [
      "#{bright("Runtime")}\n",
      "OS PID: #{m[:pid]}",
      "OS: #{m[:os]}",
      # TODO: format
      "Uptime (seconds): #{m[:uptime]}",
      "Is under maintenance?: #{m[:is_under_maintenance]}"
    ] ++
    product_name_section ++
    product_version_section ++
    [
      "RabbitMQ version: #{m[:rabbitmq_version]}",
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

    data_directory_section = [
      "\n#{bright("Data directory")}\n",
      "Node data directory: #{m[:data_directory]}",
      "Raft data directory: #{m[:raft_data_directory]}"
    ]

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
    memory_calculation_strategy = to_atom(m[:vm_memory_calculation_strategy])
    total_memory = get_in(m[:memory], [:total, memory_calculation_strategy])

    readable_watermark_setting = case m[:vm_memory_high_watermark_setting] do
      %{:relative => val} -> "#{val} of available memory"
      # absolute value
      %{:absolute => val} -> "#{IU.convert(val, unit)} #{unit}"
    end
    memory_section = [
      "\n#{bright("Memory")}\n",
      "Total memory used: #{IU.convert(total_memory, unit)} #{unit}",
      "Calculation strategy: #{memory_calculation_strategy}",
      "Memory high watermark setting: #{readable_watermark_setting}, computed to: #{IU.convert(m[:vm_memory_high_watermark_limit], unit)} #{unit}\n"
    ] ++ Enum.map(breakdown, fn({category, val}) -> "#{category}: #{IU.convert(val[:bytes], unit)} #{unit} (#{val[:percentage]} %)" end)

    file_descriptors = [
      "\n#{bright("File Descriptors")}\n",
      "Total: #{m[:file_descriptors][:total_used]}, limit: #{m[:file_descriptors][:total_limit]}",
      "Sockets: #{m[:file_descriptors][:sockets_used]}, limit: #{m[:file_descriptors][:sockets_limit]}"
    ]

    disk_space_section = [
      "\n#{bright("Free Disk Space")}\n",
      "Low free disk space watermark: #{IU.convert(m[:disk_free_limit], unit)} #{unit}",
      "Free disk space: #{IU.convert(m[:disk_free], unit)} #{unit}"
    ]

    totals_section = [
      "\n#{bright("Totals")}\n",
      "Connection count: #{m[:totals][:connection_count]}",
      "Queue count: #{m[:totals][:queue_count]}",
      "Virtual host count: #{m[:totals][:virtual_host_count]}"
    ]

    listeners_section = [
      "\n#{bright("Listeners")}\n",
    ] ++ case m[:listeners] do
           [] -> ["(none)"]
           xs -> listener_lines(xs)
         end
    lines = runtime_section ++ plugin_section ++ data_directory_section ++
            config_section ++ log_section ++ alarms_section ++ memory_section ++
            file_descriptors ++ disk_space_section ++ totals_section ++ listeners_section

    {:ok, Enum.join(lines, line_separator())}
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.String

  def usage, do: "status [--unit <unit>]"

  def usage_additional() do
    [
      ["--unit <bytes | mb | gb>", "byte multiple (bytes, megabytes, gigabytes) to use"],
      ["--formatter <json | erlang>", "alternative formatter (JSON, Erlang terms)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays status of a node"

  def banner(_, %{node: node_name}), do: "Status of node #{node_name} ..."

  #
  # Implementation
  #

  defp result_map(result) do
    %{
      os: os_name(Keyword.get(result, :os)),
      pid: Keyword.get(result, :pid),
      product_name: Keyword.get(result, :product_name) |> to_string,
      product_version: Keyword.get(result, :product_version) |> to_string,
      rabbitmq_version: Keyword.get(result, :rabbitmq_version) |> to_string,
      erlang_version: Keyword.get(result, :erlang_version) |> to_string |> String.trim_trailing,
      uptime: Keyword.get(result, :uptime),
      is_under_maintenance: Keyword.get(result, :is_under_maintenance, false),
      processes: Enum.into(Keyword.get(result, :processes), %{}),
      run_queue: Keyword.get(result, :run_queue),
      net_ticktime: net_ticktime(result),

      vm_memory_calculation_strategy: Keyword.get(result, :vm_memory_calculation_strategy),
      vm_memory_high_watermark_setting: Keyword.get(result, :vm_memory_high_watermark) |> formatted_watermark,
      vm_memory_high_watermark_limit: Keyword.get(result, :vm_memory_limit),

      disk_free_limit: Keyword.get(result, :disk_free_limit),
      disk_free: Keyword.get(result, :disk_free),

      file_descriptors: Enum.into(Keyword.get(result, :file_descriptors), %{}),

      alarms: Keyword.get(result, :alarms),
      listeners: listener_maps(Keyword.get(result, :listeners, [])),
      memory: Keyword.get(result, :memory) |> Enum.into(%{}),

      data_directory: Keyword.get(result, :data_directory) |> to_string,
      raft_data_directory: Keyword.get(result, :raft_data_directory) |> to_string,

      config_files: Keyword.get(result, :config_files) |> Enum.map(&to_string/1),
      log_files: Keyword.get(result, :log_files) |> Enum.map(&to_string/1),

      active_plugins: Keyword.get(result, :active_plugins) |> Enum.map(&to_string/1),
      enabled_plugin_file: Keyword.get(result, :enabled_plugin_file) |> to_string,

      totals: Keyword.get(result, :totals)
    }
  end

  defp net_ticktime(result) do
    case Keyword.get(result, :kernel) do
      {:net_ticktime, n}   -> n
      n when is_integer(n) -> n
      _                    -> :undefined
    end
  end
end
