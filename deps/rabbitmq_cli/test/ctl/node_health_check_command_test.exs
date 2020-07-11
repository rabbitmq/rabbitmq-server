## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule NodeHealthCheckCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.NodeHealthCheckCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    reset_vm_memory_high_watermark()

    on_exit([], fn ->
      reset_vm_memory_high_watermark()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 20000}}
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

  test "run: request to a named, active node with an alarm in effect fails", context do
    set_vm_memory_high_watermark(0.0000000000001)
    # give VM memory monitor check some time to kick in
    :timer.sleep(1500)
    {:healthcheck_failed, _message} = @command.run([], context[:opts])

    reset_vm_memory_high_watermark()
    :timer.sleep(1500)
    assert @command.run([], context[:opts]) == :ok
  end

  test "run: request to a non-existent node returns a badrpc" do
    assert match?({:badrpc, _}, @command.run([], %{node: :jake@thedog, timeout: 200}))
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) |> Enum.join("\n") =~ ~r/Checking health/
    assert @command.banner([], context[:opts]) |> Enum.join("\n") =~ ~r/#{get_rabbit_hostname()}/
  end
end
