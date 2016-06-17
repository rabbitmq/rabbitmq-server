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


defmodule CommandModulesTest do
  use ExUnit.Case, async: false

  @subject RabbitMQ.CLI.Ctl.CommandModules

  setup_all do
    source_dir = "#{Mix.Project.config[:elixirc_paths]}/fixtures"
    File.mkdir_p!(source_dir)

    filenames = [
      "#{source_dir}/imperial_command.ex",
      "#{source_dir}/lord_protectoral_command.ex",
      "#{source_dir}/polite_suggestion.ex",
      "#{source_dir}/royalcommand.ex",
      "#{source_dir}/ducal-command.ex",
      "#{source_dir}/ViceregalCommand.ex"
    ]

    Enum.map(filenames, fn(fname) -> File.touch!(fname) end)

    on_exit([], fn -> File.rm_rf!(source_dir) end)

    :ok
  end

  test "command_modules has existing commands" do
    assert @subject.module_map["imperial"] ==
      RabbitMQ.CLI.Ctl.Commands.ImperialCommand
  end

  test "command with multiple underscores shows up in map" do
    assert @subject.module_map["lord_protectoral"] ==
      RabbitMQ.CLI.Ctl.Commands.LordProtectoralCommand
  end

  test "command_modules does not have non-existent commands" do
    assert @subject.module_map["usurper"] == nil
  end

  test "non-command files do not show up in command map" do
    assert @subject.module_map["polite"] == nil
  end

  test "malformed command files do not show up in command map" do
    assert @subject.module_map["royal"] == nil
    assert @subject.module_map["ducal"] == nil
    assert @subject.module_map["viceregal"] == nil
  end
end
