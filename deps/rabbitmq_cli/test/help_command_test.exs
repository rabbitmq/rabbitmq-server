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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


defmodule HelpCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Core.{CommandModules, ExitCodes}

  @command RabbitMQ.CLI.Ctl.Commands.HelpCommand

  setup_all do
    set_scope(:all)
    :ok
  end

  test "run: prints basic usage info" do
    assert @command.run([], %{}) =~ ~r/Default node is \"rabbit@server\"/
  end

  test "run: ctl command usage info is printed if command is specified" do
    ctl_commands = CommandModules.module_map
    |> Enum.filter(fn({_name, command_mod}) ->
                     to_string(command_mod) =~ ~r/^RabbitMQ\.CLI\.Ctl\.Commands/
                   end)
    |> Enum.map(fn({name, _}) -> name end)

    IO.inspect(ctl_commands)
    Enum.each(
      ctl_commands,
      fn(command) ->
        assert @command.run([command], %{}) =~ ~r/#{command}/
      end)
  end

  test "run prints command info" do
    assert @command.run([], %{}) =~ ~r/Commands:\n/

    # Checks to verify that each module's command appears in the list.
    ctl_commands = CommandModules.module_map
    |> Enum.filter(fn({_name, command_mod}) ->
                     to_string(command_mod) =~ ~r/^RabbitMQ\.CLI\.Ctl\.Commands/
                   end)
    |> Enum.map(fn({name, _}) -> name end)

    Enum.each(
      ctl_commands,
      fn(command) ->
        assert @command.run([], %{}) =~ ~r/\n    #{command}.*\n/
      end)
  end

  test "run: sorts commands alphabetically" do
    [cmd1, cmd2, cmd3] = CommandModules.module_map
    |> Map.keys
    |> Enum.sort
    |> Enum.take(3)

    output = @command.run([], %{})

    {start1, _} = :binary.match(output, cmd1)
    {start2, _} = :binary.match(output, cmd2)
    {start3, _} = :binary.match(output, cmd3)

    assert start1 < start2
    assert start2 < start3
  end

  test "run: exits with code of OK" do
    assert @command.output("Help string", %{}) ==
      {:error, ExitCodes.exit_ok, "Help string"}
  end

  test "run: no arguments print general help" do
    assert @command.run([], %{}) =~ ~r/Usage:/
  end

  test "run: unrecognizes arguments print general help" do
    assert @command.run(["extra1", "extra2"], %{}) =~ ~r/Usage:/
  end
end
