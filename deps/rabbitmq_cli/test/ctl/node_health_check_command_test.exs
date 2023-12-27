## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

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

  test "run: is a no-op", context do
    assert @command.run([], context[:opts])
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) |> Enum.join("\n") =~ ~r/DEPRECATED/
  end
end
