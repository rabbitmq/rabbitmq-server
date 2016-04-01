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


defmodule SetDiskFreeLimitCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

  @default_limit 1048576

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)
    set_disk_free_limit(@default_limit)

    on_exit([], fn ->
      set_disk_free_limit(@default_limit)
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

  end

  setup context do
    on_exit([], fn -> set_disk_free_limit(@default_limit) end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "an invalid number of arguments results in a bad arg and usage" do
    assert capture_io(fn ->
      assert SetDiskFreeLimitCommand.set_disk_free_limit([], %{}) == {:bad_argument, []}
    end) =~ ~r/Usage:\n/

    assert capture_io(fn ->
      assert SetDiskFreeLimitCommand.set_disk_free_limit(["too", "many"], %{}) == {:bad_argument, []}
    end) =~ ~r/Usage:\n/
  end

  test "an invalid node returns a bad rpc" do
    node_name = :jake@thedog
    args = [@default_limit]
    opts = %{node: node_name}

    assert SetDiskFreeLimitCommand.set_disk_free_limit(args, opts) == {:badrpc, :nodedown}
  end

  @tag limit: 2097152
  test "a valid integer input returns an ok and sets the disk free limit", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit([context[:limit]], context[:opts]) == :ok
    assert status[:disk_free_limit] === context[:limit]
  end

  @tag limit: 2097152.0
  test "a valid non-fractional float input returns an ok and sets the disk free limit", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit([context[:limit]], context[:opts]) == :ok
    assert status[:disk_free_limit] === round(context[:limit])
  end

  @tag limit: 2097152.9
  test "a valid fractional float input returns an ok and sets the disk free limit", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit([context[:limit]], context[:opts]) == :ok
    assert status[:disk_free_limit] === context[:limit] |> Float.floor |> round
  end

  @tag limit: "2097152"
  test "an integer string input returns an ok and sets the disk free limit", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit([context[:limit]], context[:opts]) == :ok
    assert status[:disk_free_limit] === String.to_integer(context[:limit])
  end

  @tag limit: "2097152bytes"
  test "an invalid string input returns a bad arg and does not change the limit", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit([context[:limit]], context[:opts]) == 
      {:bad_argument, [context[:limit]]}
    assert status[:disk_free_limit] === @default_limit
  end

  @tag limit: "2MB"
  test "an valid unit string input returns an ok and changes the limit", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit([context[:limit]], context[:opts]) == :ok
    assert status[:disk_free_limit] === 2000000
  end

## ------------------------ implement relative command -------------------------------------------

  test "an invalid number of mem_relative arguments results in a bad arg and usage" do
    assert capture_io(fn ->
      assert SetDiskFreeLimitCommand.set_disk_free_limit(["mem_relative"], %{}) == {:bad_argument, []}
    end) =~ ~r/Usage:\n/

    assert capture_io(fn ->
      assert SetDiskFreeLimitCommand.set_disk_free_limit(["mem_relative", 1.3, "extra"], %{}) == {:bad_argument, ["extra"]}
    end) =~ ~r/Usage:\n/
  end

  test "valid fractional inputs return an ok", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", 0.0],
      context[:opts]
    ) == :ok

    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", 0.5],
      context[:opts]
    ) == :ok

    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", 1.8],
      context[:opts]
    ) == :ok
  end

  test "a value outside the accepted range returns an error", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", -1.0],
      context[:opts]
    ) == {:bad_argument, -1.0}
  end

  @tag fraction: "1.3"
  test "a valid float string input returns ok", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == :ok
  end

  @tag fraction: "1.3salt"
  test "an invalid string input returns a bad argument", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == {:bad_argument, [context[:fraction]]}

    assert status[:disk_free_limit] === @default_limit
  end

  @tag fraction: 1
  test "an integer input returns ok", context do
    assert SetDiskFreeLimitCommand.set_disk_free_limit(
      ["mem_relative", context[:fraction]],
      context[:opts]
    ) == :ok
  end
end
