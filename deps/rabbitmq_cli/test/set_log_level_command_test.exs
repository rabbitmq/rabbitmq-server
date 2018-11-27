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


defmodule SetLogLevelCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetLogLevelCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    {:ok,
      log_level: "debug",
      opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: with one argument succeeds", context do
    assert @command.validate([context[:log_level]], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate([context[:log_level], "whoops"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "run: request to a named, active node succeeds", context do
    assert @command.run([context[:log_level]], context[:opts]) == :ok
  end

  test "run: request to a non-existent node returns nodedown", context do
    target = :jake@thedog
    opts = %{node: target}
    assert match?({:badrpc, :nodedown}, @command.run([context[:log_level]], opts))
  end

  test "banner", context do
    assert @command.banner([context[:log_level]], context[:opts]) == "Setting log level to \"debug\" ..."
  end
end
