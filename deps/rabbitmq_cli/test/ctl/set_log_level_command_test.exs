## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

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

  test "validate: with a single known level succeeds", context do
    assert @command.validate([context[:log_level]], context[:opts]) == :ok
  end

  test "validate: with a single unsupported level fails", context do
    assert match?({:error, _}, @command.validate(["lolwut"], context[:opts]))
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate([context[:log_level], "whoops"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "run: request to a named, active node succeeds", context do
    assert @command.run([context[:log_level]], context[:opts]) == :ok
  end

  test "run: request to a non-existent node returns a badrpc", context do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([context[:log_level]], opts))
  end

  test "banner", context do
    assert @command.banner([context[:log_level]], context[:opts]) == "Setting log level to \"debug\" ..."
  end
end
