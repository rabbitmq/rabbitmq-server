## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule StopCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.StopCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(),
                  idempotent: false}}
  end

  test "validate accepts no arguments", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate accepts a PID file path", context do
    assert @command.validate(["/path/to/pidfile.pid"], context[:opts]) == :ok
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["/path/to/pidfile.pid", "extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  # NB: as this commands shuts down the Erlang vm it isn't really practical to test it here

  test "run: request to a non-existent node with --idempotent=false returns a badrpc" do
    opts = %{node: :jake@thedog, idempotent: false, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "run: request to a non-existent node with --idempotent returns ok" do
    opts = %{node: :jake@thedog, idempotent: true, timeout: 200}
    assert match?({:ok, _}, @command.run([], opts))
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) =~ ~r/Stopping and halting node #{get_rabbit_hostname()}/
  end
end
