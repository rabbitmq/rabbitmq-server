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


defmodule HelpCommandTest do
  use ExUnit.Case, async: false

  alias RabbitMQ.CLI.Core.Helpers, as: Helpers
  alias RabbitMQ.CLI.Core.ExitCodes,   as: ExitCodes

  @command RabbitMQ.CLI.Ctl.Commands.HelpCommand

  setup_all do
    :ok
  end

  test "basic usage info is printed" do
    assert @command.run([], %{}) =~ ~r/Default node is \"rabbit@server\"/
  end

  test "command usage info is printed if command is specified" do
    Helpers.commands
    |>  Map.keys
    |>  Enum.each(
          fn(command) ->
            assert @command.run([command], %{}) =~ ~r/#{command}/
          end)
  end

  test "Command info is printed" do
    assert @command.run([], %{}) =~ ~r/Commands:\n/

    # Checks to verify that each module's command appears in the list.
    Helpers.commands
    |>  Map.keys
    |>  Enum.each(
          fn(command) ->
            assert @command.run([], %{}) =~ ~r/\n    #{command}.*\n/
          end)
  end

  test "Info items are defined for existing commands" do
    assert @command.run([], %{}) =~ ~r/\n\<vhostinfoitem\> .*\n/
    assert @command.run([], %{}) =~ ~r/\n\<queueinfoitem\> .*\n/
    assert @command.run([], %{}) =~ ~r/\n\<exchangeinfoitem\> .*\n/
    assert @command.run([], %{}) =~ ~r/\n\<bindinginfoitem\> .*\n/
    assert @command.run([], %{}) =~ ~r/\n\<connectioninfoitem\> .*\n/
    assert @command.run([], %{}) =~ ~r/\n\<channelinfoitem\> .*\n/
  end

  test "Info items are printed for selected command" do
    assert @command.run(["list_vhosts"], %{}) =~ ~r/\n\<vhostinfoitem\> .*/
    assert @command.run(["list_queues"], %{}) =~ ~r/\n\<queueinfoitem\> .*/
    assert @command.run(["list_exchanges"], %{}) =~ ~r/\n\<exchangeinfoitem\> .*/
    assert @command.run(["list_bindings"], %{}) =~ ~r/\n\<bindinginfoitem\> .*/
    assert @command.run(["list_connections"], %{}) =~ ~r/\n\<connectioninfoitem\> .*/
    assert @command.run(["list_channels"], %{}) =~ ~r/\n\<channelinfoitem\> .*/
  end

  test "Help command returns exit code OK" do
    assert @command.output("Help string", %{}) ==
      {:error, ExitCodes.exit_ok, "Help string"}
  end

  test "No arguments also produce help command" do
    assert @command.run([], %{}) =~ ~r/Usage:/
  end

  test "Extra arguments also produce help command" do
    assert @command.run(["extra1", "extra2"], %{}) =~ ~r/Usage:/
  end
end
