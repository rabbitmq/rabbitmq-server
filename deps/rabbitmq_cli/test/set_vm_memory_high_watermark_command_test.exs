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


defmodule SetVmMemoryHighWatermarkCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  import SetVmMemoryHighWatermarkCommand

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :vm_memory_monitor.set_vm_memory_high_watermark(0.4)

      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "a string returns an error", context do
    capture_io(fn ->
      assert run(["sandwich"], context[:opts]) == {:bad_argument, ["sandwich"]}
      assert run(["0.4sandwich"], context[:opts]) == {:bad_argument, ["0.4sandwich"]}
    end)
  end

  test "a valid numerical value returns ok", context do
    capture_io(fn ->
      assert run([0.7], context[:opts]) == :ok
      assert status[:vm_memory_high_watermark] == 0.7

      assert run([1], context[:opts]) == :ok
      assert status[:vm_memory_high_watermark] == 1
    end)
  end

  test "on an invalid node, return a bad rpc" do
    node_name = :jake@thedog
    args = [0.7]
    opts = %{node: node_name}

    capture_io(fn ->
      assert run(args, opts) == {:badrpc, :nodedown}
    end)
  end

  test "a valid numerical string value returns ok", context do
    capture_io(fn ->
      assert run(["0.7"], context[:opts]) == :ok
      assert status[:vm_memory_high_watermark] == 0.7

      assert run(["1"], context[:opts]) == :ok
      assert status[:vm_memory_high_watermark] == 1
    end)
  end

  test "the wrong number of arguments returns an arg count error" do
    capture_io(fn ->
      assert run([], %{}) == {:not_enough_args, []}
      assert run(["too", "many"], %{}) == {:too_many_args, ["too", "many"]}
    end)
  end

  test "a negative number returns a bad argument", context do
    capture_io(fn ->
      assert run([-0.1], context[:opts]) == {:bad_argument, [-0.1]}
    end)
  end

  test "a value greater than 1.0 returns a bad argument", context do
    capture_io(fn ->
      assert run([1.1], context[:opts]) == {:bad_argument, [1.1]}
    end)
  end

## ---------------------------- Absolute tests --------------------------------

  test "an absolute call without an argument returns not enough args" do
    capture_io(fn ->
      assert run(["absolute"], %{}) == {:not_enough_args, ["absolute"]}
    end)
  end

  test "an absolute call with too many arguments returns too many args" do
    capture_io(fn ->
      assert run(["absolute", "too", "many"], %{}) ==
        {:too_many_args, ["absolute", "too", "many"]}
    end)
  end

  test "a single absolute integer return ok", context do
    capture_io(fn ->
      assert run(["absolute","10"], context[:opts]) == :ok
      assert status[:vm_memory_high_watermark] == {:absolute, Helpers.memory_unit_absolute(10, "")}
    end)
  end

  test "a single absolute integer with memory units return ok", context do
    Helpers.memory_units
    |> Enum.each(fn mu ->
      arg = "10#{mu}"
      capture_io(fn ->
        assert run(["absolute",arg], context[:opts]) == :ok
      end)
      assert status[:vm_memory_high_watermark] == {:absolute, Helpers.memory_unit_absolute(10, mu)}
    end)
  end

  test "a single absolute integer with an invalid memory unit fails ", context do
    capture_io(fn ->
      assert run(["absolute","10bytes"], context[:opts]) == {:bad_argument, ["10bytes"]}
    end)
  end

  test "a single absolute string fails ", context do
    capture_io(fn ->
      assert run(["absolute","large"], context[:opts]) == {:bad_argument, ["large"]}
    end)
  end

  test "a single absolute string with a valid unit  fails ", context do
    capture_io(fn ->
      assert run(["absolute","manyGB"], context[:opts]) == {:bad_argument, ["manyGB"]}
    end)
  end

  test "by default, absolute memory request prints info message", context do
    assert capture_io(fn ->
      run(["absolute", "10"], context[:opts])
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to 10 bytes .../

    assert capture_io(fn ->
      run(["absolute", "-10"], context[:opts])
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to -10 bytes .../

    assert capture_io(fn ->
      run(["absolute", "sandwich"], context[:opts])
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to sandwich bytes .../
  end

  test "by default, relative memory request prints info message", context do
    assert capture_io(fn ->
      run(["0.7"], context[:opts])
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to 0.7 .../

    assert capture_io(fn ->
      run(["-0.7"], context[:opts])
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to -0.7 .../

    assert capture_io(fn ->
      run(["sandwich"], context[:opts])
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to sandwich .../
  end

  test "the quiet flag suppresses the info message", context do
    opts = Map.merge(context[:opts], %{quiet: true})

    refute capture_io(fn ->
      run(["0.7"], opts)
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to 0.7 .../

    refute capture_io(fn ->
      run(["absolute", "10"], opts)
    end) =~ ~r/Setting memory threshold on #{get_rabbit_hostname} to 10 bytes .../
  end
end
