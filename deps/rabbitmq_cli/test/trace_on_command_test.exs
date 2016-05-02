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


defmodule TraceOnCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @test_vhost "test"
  @default_vhost "/"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)
    add_vhost(@test_vhost)

    on_exit([], fn ->
      delete_vhost(@test_vhost)
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    on_exit(context, fn -> trace_off(context[:vhost]) end)
    {:ok, opts: %{node: get_rabbit_hostname, param: context[:vhost]}}
  end

  test "wrong number of arguments triggers arg count error" do
    assert TraceOnCommand.trace_on(["extra"], %{}) == {:too_many_args, ["extra"]}
  end

  test "on an active node, trace_on command works on default" do
    opts = %{node: get_rabbit_hostname}

    capture_io(fn ->
      assert TraceOnCommand.trace_on([], opts) == :ok
    end)

    trace_off(@default_vhost)
  end

  test "on an invalid RabbitMQ node, return a nodedown" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    capture_io(fn ->
      assert TraceOnCommand.trace_on([], opts) == {:badrpc, :nodedown}
    end)
  end

  @tag vhost: @default_vhost
  test "calls to trace_on are idempotent", context do
    capture_io(fn -> TraceOnCommand.trace_on([], context[:opts]) end)

    capture_io(fn ->
      assert TraceOnCommand.trace_on([], context[:opts]) == :ok
    end)
  end

  @tag vhost: @test_vhost
  test "on an active node, trace_on command works on named vhost", context do
    capture_io(fn ->
      assert TraceOnCommand.trace_on([], context[:opts]) == :ok
    end)
  end

  @tag vhost: "toast"
  test "Turning tracing off on invalid host returns successfully", context do
    capture_io(fn ->
      assert TraceOnCommand.trace_on([], context[:opts]) == :ok
    end)
  end

  @tag vhost: @default_vhost
  test "by default, trace_on prints an info message", context do
    assert capture_io(fn ->
      TraceOnCommand.trace_on([], context[:opts])
    end) =~ ~r/Starting tracing for vhost "#{context[:vhost]}" .../
  end

  @tag vhost: @default_vhost
  test "the quiet flag suppresses the info message", context do
    opts = Map.merge(context[:opts], %{quiet: true})

    refute capture_io(fn ->
      TraceOnCommand.trace_on([], opts)
    end) =~ ~r/Starting tracing for vhost "#{context[:vhost]}" .../
  end
end
