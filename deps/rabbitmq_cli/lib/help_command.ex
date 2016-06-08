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
  def validate(_, _), do: :ok
  def merge_defaults(args, opts), do: {args, opts}

  def switches(), do: []

  def run([command_name], _) do
    case Helpers.is_command?(command_name) do
      true  ->
        command = Helpers.commands[command_name]
        print_base_usage(command)
        print_options_usage
        print_input_types(command);
      false ->
        all_usage()
        ExitCodes.exit_usage
    end
  end

  def run(_, _) do
    all_usage()
  end

  def all_usage() do
    print_base_usage
    print_options_usage
    print_commands
    print_input_types
    :ok
  end

  def usage(), do: "help <command>"

  defp print_base_usage() do
    IO.puts "Usage:"
    IO.puts "rabbitmqctl [-n <node>] [-t <timeout>] [-l] [-q] <command> [<command options>]"
  end

  def print_base_usage(command) do
    IO.puts "Usage:"
    IO.puts "rabbitmqctl [-n <node>] [-t <timeout>] [-q] " <>
    flatten_string(command.usage())
  end

  defp flatten_string(list) when is_list(list) do
    Enum.join(list, "\n")
  end
  defp flatten_string(str) when is_binary(str) do
    str
  end

  defp print_options_usage() do
    IO.puts "
Options:
    -n node
    -q
    -t timeout
    -l longnames

Default node is \"rabbit@server\", where server is the local host. On a host
named \"server.example.com\", the node name of the RabbitMQ Erlang node will
usually be rabbit@server (unless RABBITMQ_NODENAME has been set to some
non-default value at broker startup time). The output of hostname -s is usually
the correct suffix to use after the \"@\" sign. See rabbitmq-server(1) for
details of configuring the RabbitMQ broker.

Quiet output mode is selected with the \"-q\" flag. Informational messages are
suppressed when quiet mode is in effect.

Operation timeout in seconds. Only applicable to \"list\" commands. Default is
\"infinity\".

If RabbitMQ broker uses long node names for erlang distribution, \"longnames\"
option should be specified.

Some commands accept an optional virtual host parameter for which
to display results. The default value is \"/\".\n"
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

  defp print_input_types(command) do
    if :erlang.function_exported(command, :usage_additional, 0) do
      IO.puts(command.usage_additional())
    else
      :ok
    end
  end

  defp print_input_types() do
    Helpers.commands
    |> Map.values
    |> Enum.filter_map(
        &:erlang.function_exported(&1, :usage_additional, 0),
        &(&1.usage_additional))
    |> Enum.join("\n\n")
    |> IO.puts
  end

  def banner(_,_), do: nil
  def flags, do: @flags
end
