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


defmodule StatusCommand do
  import Helpers
  import TablePrint

  @otp_version_tag "otp_version"
  @erts_version_tag "erts_version"


  def status(options) do
    case options[:node] do
      nil -> get_rabbit_hostname |> :rpc.call(:rabbit, :status, [])
      host when is_atom(host) -> host |> :rpc.call(:rabbit, :status, [])
      host when is_binary(host) -> host |> String.to_atom() |> :rpc.call(:rabbit, :status, [])
    end
  end

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
  end

  defp print_os(result) when not is_list(result), do: result
  defp print_os(result) when is_list(result) do
    case result[:os] do
      nil -> nil
      _ -> IO.puts "OS: #{os_name}"
    end
    result
  end

  defp print_pid(result) when not is_list(result), do: result
  defp print_pid(result) when is_list(result) do
    case result[:pid] do
      nil -> nil
      _ -> IO.puts "PID: #{result[:pid]}"
    end
    result
  end

  defp print_otp_version(result) when not is_list(result), do: result
  defp print_otp_version(result) when is_list(result) do
    case erl = result[:erlang_version] do
      nil -> nil
      _ -> IO.puts "OTP version: #{otp_version_number(to_string(erl))}"
    end
    result
  end

  defp print_erts_version(result) when not is_list(result), do: result
  defp print_erts_version(result) when is_list(result) do
    case erl = result[:erlang_version] do
      nil -> nil
      _ -> IO.puts "Erlang RTS version: #{erts_version_number(to_string(erl))}"
    end
    result
  end

  defp print_running_apps(result) do
    print_table(result, :running_applications, "Applications currently running")
  end

  defp print_memory_usage(result) do
    print_table(result, :memory, "Memory usage")
  end

  defp print_line_break(result) when not is_list(result), do: result
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
