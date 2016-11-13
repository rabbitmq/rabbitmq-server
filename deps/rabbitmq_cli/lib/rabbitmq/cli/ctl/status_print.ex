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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.StatusPrint do
  import RabbitMQ.CLI.Ctl.TablePrint

  @otp_version_tag "otp_version"
  @erts_version_tag "erts_version"

  def print_status(result) when not is_list(result), do: result
  def print_status(result) do
    result
    |> print_pid
    |> print_os
    |> print_line_break
    |> print_otp_version
    |> print_erts_version
    |> print_line_break
    |> print_running_apps
    |> print_line_break
    |> print_memory_usage
    |> print_line_break
    |> print_alarms
    |> print_line_break
    |> print_listeners
    |> print_line_break
    |> print_memory_high_watermark
    |> print_memory_limit
    |> print_line_break
    |> print_disk_free_limit
    |> print_disk_free
    |> print_line_break
    |> print_file_descriptor_data
    |> print_line_break
    |> print_process_data
    |> print_line_break
    |> print_run_queue
    |> print_line_break
    |> print_uptime
    |> print_line_break
    |> print_ticktime
  end

  defp print_os(result) when is_list(result) do
    case result[:os] do
      nil -> nil
      _ -> IO.puts "OS: #{os_name}"
    end
    result
  end

  defp print_pid(result) when is_list(result) do
    case result[:pid] do
      nil -> nil
      _ -> IO.puts "PID: #{result[:pid]}"
    end
    result
  end

  defp print_otp_version(result) when is_list(result) do
    case erl = result[:erlang_version] do
      nil -> nil
      _ -> IO.puts "OTP version: #{otp_version_number(to_string(erl))}"
    end
    result
  end

  defp print_erts_version(result) when is_list(result) do
    case erl = result[:erlang_version] do
      nil -> nil
      _ -> IO.puts "Erlang RTS version: #{erts_version_number(to_string(erl))}"
    end
    result
  end

  defp print_running_apps(result) do
    print_table(result, :running_applications, "Applications currently running")
    result
  end

  defp print_memory_usage(result) do
    print_table(result, :memory, "Memory usage")
    result
  end

  defp print_alarms(result) do
    print_table(result, :alarms, "Resource Alarms")
    result
  end

  defp print_listeners(result) do
    print_table(result, :listeners, "Listeners")
    result
  end

  defp print_memory_high_watermark(result) do
    case watermark = result[:vm_memory_high_watermark] do
      nil -> nil
      _ -> IO.puts "VM Memory High Water Mark: #{watermark}"
    end
    result
  end

  defp print_memory_limit(result) do
    case mem_limit = result[:vm_memory_limit] do
      nil -> nil
      _ -> IO.puts "VM Memory Limit: #{mem_limit}"
    end
    result
  end

  defp print_disk_free_limit(result) do
    case disk_limit = result[:disk_free_limit] do
      nil -> nil
      _ -> IO.puts "Disk Free Limit: #{disk_limit}"
    end
    result
  end

  defp print_disk_free(result) do
    case disk_free = result[:disk_free] do
      nil -> nil
      _ -> IO.puts "Disk Free: #{disk_free}"
    end
    result
  end

  defp print_file_descriptor_data(result) do
    print_table(result, :file_descriptors, "File Descriptor Stats")
    result
  end

  defp print_process_data(result) do
    print_table(result, :processes, "RabbitMQ Process Stats")
    result
  end

  defp print_run_queue(result) do
    case run_queue = result[:run_queue] do
      nil -> nil
      _ -> IO.puts "Run Queue: #{run_queue}"
    end
    result
  end

  defp print_uptime(result) do
    case uptime = result[:uptime] do
      nil -> nil
      _ -> IO.puts "Broker Uptime: #{uptime}"
    end
    result
  end

  defp print_ticktime(result) do
    case result[:kernel] do
      {:net_ticktime, tick} -> IO.puts "Network Tick Time: #{tick}"
      _ -> nil
    end
    result
  end

  defp print_line_break(result) do
    IO.puts ""
    result
  end
#----------------------- Helper functions --------------------------------------

  defp os_name do
    :os.type
    |> elem(1)
    |> Atom.to_string
    |> Mix.Utils.camelize
  end

  defp otp_version_number(erlang_string) do
    ~r/OTP (?<#{@otp_version_tag}>\d+)/
    |> Regex.named_captures(erlang_string)
    |> Map.fetch!(@otp_version_tag)
  end

  defp erts_version_number(erlang_string) do
    ~r/\[erts\-(?<#{@erts_version_tag}>\d+\.\d+\.\d+)\]/
    |> Regex.named_captures(erlang_string)
    |> Map.fetch!(@erts_version_tag)
  end
end
