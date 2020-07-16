## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule PingCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.PingCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    reset_vm_memory_high_watermark()

    on_exit([], fn ->
      reset_vm_memory_high_watermark()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 200}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "validate: with no arguments succeeds", _context do
    assert @command.validate([], []) == :ok
  end

  test "validate: with a named, active node argument succeeds", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "run: request to a named, active node succeeds", context do
    assert @command.run([], context[:opts])
  end

  test "run: request to a non-existent node returns a badrpc" do
    assert match?({:error, _}, @command.run([], %{node: :jake@thedog, timeout: 200}))
  end

  test "banner", context do
    banner  = @command.banner([], context[:opts])

    assert banner =~ ~r/Will ping/
    assert banner =~ ~r/#{get_rabbit_hostname()}/
  end
end
