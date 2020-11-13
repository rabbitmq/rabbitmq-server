## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetDiskFreeLimitCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetDiskFreeLimitCommand

  @default_limit 1048576

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    set_disk_free_limit(@default_limit)

    on_exit([], fn ->
      set_disk_free_limit(@default_limit)
    end)

  end

  setup context do
    context[:tag] # silences warnings
    on_exit([], fn -> set_disk_free_limit(@default_limit) end)

    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: an invalid number of arguments results in arg count errors" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag limit: "2097152bytes"
  test "run: an invalid string input returns a bad arg and does not change the limit", context do
    assert @command.validate([context[:limit]], context[:opts]) ==
      {:validation_failure, :bad_argument}
  end

  test "validate: valid fractional inputs return an ok", context do
    assert @command.validate(
      ["mem_relative", "0.0"],
      context[:opts]
    ) == :ok

    assert @command.validate(
      ["mem_relative", "0.5"],
      context[:opts]
    ) == :ok

    assert @command.validate(
      ["mem_relative", "1.8"],
      context[:opts]
    ) == :ok
  end

  test "validate: a value outside the accepted range returns an error", context do
      assert @command.validate(
        ["mem_relative", "-1.0"],
        context[:opts]
      ) == {:validation_failure, :bad_argument}
  end

  @tag fraction: "1.3"
  test "validate: a valid float string input returns ok", context do
    assert @command.validate(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == :ok
  end

  @tag fraction: "1.3salt"
  test "validate: an invalid string input returns a bad argument", context do
    assert @command.validate(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == {:validation_failure, :bad_argument}
  end

## ------------------------ validate mem_relative command -------------------------------------------

  test "validate: an invalid number of mem_relative arguments results in an arg count error" do
    assert @command.validate(["mem_relative"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["mem_relative", 1.3, "extra"], %{}) == {:validation_failure, :too_many_args}
  end


## ------------------------ run absolute command -------------------------------------------

  @tag test_timeout: 3000
  test "run: an invalid node returns a bad rpc" do
    args = [@default_limit]
    opts = %{node: :jake@thedog}

    assert match?({:badrpc, _}, @command.run(args, opts))
  end

  @tag limit: 2097152
  test "run: a valid integer input returns an ok and sets the disk free limit", context do
    assert @command.run([context[:limit]], context[:opts]) == :ok
    assert status()[:disk_free_limit] === context[:limit]
  end

  @tag limit: 2097152.0
  test "run: a valid non-fractional float input returns an ok and sets the disk free limit", context do
    assert @command.run([context[:limit]], context[:opts]) == :ok
    assert status()[:disk_free_limit] === round(context[:limit])
  end

  @tag limit: 2097152.9
  test "run: a valid fractional float input returns an ok and sets the disk free limit", context do
    assert @command.run([context[:limit]], context[:opts]) == :ok
    assert status()[:disk_free_limit] === context[:limit] |> Float.floor |> round
  end

  @tag limit: "2097152"
  test "run: an integer string input returns an ok and sets the disk free limit", context do
    assert @command.run([context[:limit]], context[:opts]) == :ok
    assert status()[:disk_free_limit] === String.to_integer(context[:limit])
  end

  @tag limit: "2MB"
  test "run: an valid unit string input returns an ok and changes the limit", context do
    assert @command.run([context[:limit]], context[:opts]) == :ok
    assert status()[:disk_free_limit] === 2000000
  end

## ------------------------ run relative command -------------------------------------------

  @tag fraction: 1
  test "run: an integer input returns ok", context do
    assert @command.run(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == :ok
  end

  @tag fraction: 1.1
  test "run: a factional  input returns ok", context do
    assert @command.run(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == :ok
  end


  test "banner: returns absolute message", context do
    assert @command.banner(["10"], context[:opts])
      =~ ~r/Setting disk free limit on #{get_rabbit_hostname()} to 10 bytes .../

    assert @command.banner(["-10"], context[:opts])
      =~ ~r/Setting disk free limit on #{get_rabbit_hostname()} to -10 bytes .../

    assert @command.banner(["sandwich"], context[:opts])
      =~ ~r/Setting disk free limit on #{get_rabbit_hostname()} to sandwich bytes .../
  end

  test "banner: returns memory-relative message", context do
    assert @command.banner(["mem_relative", "1.3"], context[:opts])
      =~ ~r/Setting disk free limit on #{get_rabbit_hostname()} to 1\.3 times the total RAM \.\.\./

    assert @command.banner(["mem_relative", "-1.3"], context[:opts])
      =~ ~r/Setting disk free limit on #{get_rabbit_hostname()} to -1\.3 times the total RAM \.\.\./

    assert @command.banner(["mem_relative", "sandwich"], context[:opts])
      =~ ~r/Setting disk free limit on #{get_rabbit_hostname()} to sandwich times the total RAM \.\.\./
  end
end
