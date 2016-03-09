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


defmodule TraceOffCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup default_context do
    :net_kernel.connect_node(default_context[:target])
    trace_on(default_context[:vhost])

    on_exit(default_context, fn ->
      :erlang.disconnect_node(default_context[:target])
    end)
    {:ok, opts: %{node: default_context[:target]}}
  end

  setup vhost_context do
    :net_kernel.connect_node(vhost_context[:target])
    add_vhost(vhost_context[:param])
    trace_on(vhost_context[:vhost])

    on_exit(vhost_context, fn ->
      delete_vhost(vhost_context[:vhost])
      :erlang.disconnect_node(vhost_context[:target])
    end)
    {:ok, opts: %{node: vhost_context[:target], param: vhost_context[:vhost]}}
  end

  test "wrong number of arguments triggers usage" do
    assert capture_io(fn ->
      TraceOffCommand.trace_off(["extra"], %{})
    end) =~ ~r/Usage:/
  end

  @tag target: get_rabbit_hostname
  test "on an active node, trace_off command works on default", default_context do
    assert TraceOffCommand.trace_off([], default_context[:opts]) == :ok
  end

  @tag target: get_rabbit_hostname
  test "calls to trace_off are idempotent", default_context do
    TraceOffCommand.trace_off([], default_context[:opts])
    assert TraceOffCommand.trace_off([], default_context[:opts]) == :ok
  end

  @tag target: :jake@thedog
  test "on an invalid RabbitMQ node, return a nodedown", default_context do
    assert TraceOffCommand.trace_off([], default_context[:opts]) == {:badrpc, :nodedown}
  end

  @tag target: get_rabbit_hostname, vhost: "test"
  test "on an active node, trace_off command works on named vhost", vhost_context do
    assert TraceOffCommand.trace_off([], vhost_context[:opts]) == :ok
  end
end
