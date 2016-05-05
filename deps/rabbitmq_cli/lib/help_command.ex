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


defmodule HelpCommand do

  @behaviour CommandBehaviour
  @flags []

  def run(_, _), do: run
  def run() do
    print_base_usage
    print_commands
    print_input_types
    :ok
  end

  def usage(), do: "help"

  def command_usage(cmd_name) do
    command = Helpers.commands[cmd_name]
    command.usage <> maybe_usage_additional(command)
    |> format_usage
  end

  defp maybe_usage_additional(command) do
    if :erlang.function_exported(command, :usage_additional, 0) do
        command.usage_additional
    end
  end

  defp format_usage(usage) when is_binary(usage), do: "\t" <> usage
  defp format_usage([_|_] = usage) do
    usage
    |> Enum.map(fn usage_str -> "\t" <> usage_str end)
    |> Enum.join("\n")
  end

  defp print_base_usage() do
    IO.puts "Usage:
rabbitmqctl [-n <node>] [-t <timeout>] [-q] <command> [<command options>]

Options:
    -n node
    -q
    -t timeout

Default node is \"rabbit@server\", where server is the local host. On a host
named \"server.example.com\", the node name of the RabbitMQ Erlang node will
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some
non-default value at broker startup time). The output of hostname -s is usually
the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for
details of configuring the RabbitMQ broker.

Quiet output mode is selected with the \"-q\" flag. Informational messages are
suppressed when quiet mode is in effect.

Operation timeout in seconds. Only applicable to \"list\" commands. Default is
\"infinity\".\n"
  end

  defp print_commands() do
    IO.puts "Commands:"

    # Enum.map obtains the usage string for each command module.
    # Enum.each prints them all.
    Helpers.commands
    |>  Map.values
    |>  Enum.map(&(&1.usage))
    |>  List.flatten
    |>  Enum.each(fn(cmd_usage) -> IO.puts "    #{cmd_usage}" end)

    :ok
  end

  defp print_input_types() do
    IO.puts "\n"

    Helpers.commands
    |> Map.values
    |> Enum.filter_map(
        &:erlang.function_exported(&1, :usage_additional, 0),
        &(&1.usage_additional))
    |> Enum.join("\n\n")
    |> IO.puts
    IO.puts "\n
The list_queues, list_exchanges and list_bindings commands accept an optional
virtual host parameter for which to display results. The default value is \"/\"."
  end

  def flags, do: @flags
end
