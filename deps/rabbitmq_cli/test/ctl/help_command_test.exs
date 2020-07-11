## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule HelpCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Core.{CommandModules}

  @command RabbitMQ.CLI.Ctl.Commands.HelpCommand

  setup_all do
    set_scope(:all)
    :ok
  end

  test "validate: providing no position arguments passes validation" do
    assert @command.validate([], %{}) == :ok
  end

  test "validate: providing one position argument passes validation" do
    assert @command.validate(["status"], %{}) == :ok
  end

  test "validate: providing two or more position arguments fails validation" do
    assert @command.validate(["extra1", "extra2"], %{}) ==
      {:validation_failure, :too_many_args}
  end

  test "run: prints basic usage info" do
    {:ok, lines} = @command.run([], %{})
    output = Enum.join(lines, "\n")
    assert output =~ ~r/[-n <node>] [-t <timeout>]/
    assert output =~ ~r/commands/i
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
    ctl_commands = CommandModules.module_map
    |> Enum.filter(fn({_name, command_mod}) ->
                     to_string(command_mod) =~ ~r/^RabbitMQ\.CLI\.Ctl\.Commands/
                   end)
    |> Enum.map(fn({name, _}) -> name end)

    Enum.each(
      ctl_commands,
      fn(command) ->
        {:ok, lines} = @command.run([], %{})
        output = Enum.join(lines, "\n")
        assert output =~ ~r/\n\s+#{command}.*\n/
      end)
  end

  test "run: exits with the code of OK" do
    assert @command.output({:ok, "Help string"}, %{}) ==
      {:ok, "Help string"}
  end
end
