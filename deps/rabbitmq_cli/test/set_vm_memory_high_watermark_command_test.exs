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
  import TestHelper
  import ExUnit.CaptureIO

  import SetVmMemoryHighWatermarkCommand

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "a string returns an error", context do
    assert set_vm_memory_high_watermark(["sandwich"], context[:opts]) == {:bad_argument, ["sandwich"]}
  end

  test "a valid numerical value returns ok", context do
    assert set_vm_memory_high_watermark([0.7], context[:opts]) == :ok

    assert status[:vm_memory_high_watermark] == 0.7

    assert set_vm_memory_high_watermark([1], context[:opts]) == :ok

    assert status[:vm_memory_high_watermark] == 1
  end

  test "on an invalid node, return a bad rpc" do
    node_name = :jake@thedog
    args = [0.7]
    opts = %{node: node_name}

    assert set_vm_memory_high_watermark(args, opts) == {:badrpc, :nodedown}
  end

  test "a valid numerical string value returns ok", context do
    assert set_vm_memory_high_watermark(["0.7"], context[:opts]) == :ok
    assert status[:vm_memory_high_watermark] == 0.7

    assert set_vm_memory_high_watermark(["1"], context[:opts]) == :ok
    assert status[:vm_memory_high_watermark] == 1
  end

  test "the wrong number of arguments returns usage" do
    assert capture_io(fn ->
      assert set_vm_memory_high_watermark([], %{}) == {:bad_argument, []}
    end) =~ ~r/Usage:\n/

    assert capture_io(fn ->
      assert set_vm_memory_high_watermark(["too", "many"], %{}) == {:bad_argument, ["too many arguments"]}
    end) =~ ~r/Usage:\n/
  end

  test "a negative number returns a bad argument", context do
    assert set_vm_memory_high_watermark([-1.01], context[:opts]) == {:bad_argument, [-1.01]}
  end

  test "a single absolute integer return ok", context do
    assert set_vm_memory_high_watermark(["absolute","10"], context[:opts]) == :ok
    assert status[:vm_memory_high_watermark] == {:absolute, Helpers.memory_unit_absolute(10, "")}
  end

  test "a single absolute integer with memory units return ok", context do
    Helpers.memory_units
    |> Enum.each(fn mu ->
      arg = "10#{mu}"
      assert set_vm_memory_high_watermark(["absolute",arg], context[:opts]) == :ok
      assert status[:vm_memory_high_watermark] == {:absolute, Helpers.memory_unit_absolute(10, mu)}
    end)
  end

  test "a single absolute integer with an invalid memory unit fails ", context do
    assert set_vm_memory_high_watermark(["absolute","10bytes"], context[:opts]) == {:bad_argument, ["10bytes"]}
  end

  test "a single absolute string fails ", context do
    assert set_vm_memory_high_watermark(["absolute","large"], context[:opts]) == {:bad_argument, ["large"]}
  end

  test "a single absolute string with a valid unit  fails ", context do
    assert set_vm_memory_high_watermark(["absolute","manyGB"], context[:opts]) == {:bad_argument, ["manyGB"]}
  end

end
