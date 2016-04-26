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
  import TestHelper

  @test_vhost "test"

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

  setup default_context do
    trace_on(default_context[:vhost])
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  setup vhost_context do
    trace_on(vhost_context[:vhost])
    {:ok, opts: %{node: get_rabbit_hostname, param: vhost_context[:vhost]}}
  end

  test "wrong number of arguments triggers arg count error" do
    assert TraceOffCommand.trace_off(["extra"], %{}) == {:too_many_args, ["extra"]}
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

  test "on an invalid RabbitMQ node, return a nodedown" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert TraceOffCommand.trace_off([], opts) == {:badrpc, :nodedown}
  end

  @tag vhost: "test"
  test "on an active node, trace_off command works on named vhost", vhost_context do
    assert TraceOffCommand.trace_off([], vhost_context[:opts]) == :ok
  end

  @tag vhost: "toast"
  test "Turning tracing off on invalid host returns successfully", vhost_context do
    assert TraceOffCommand.trace_off([], vhost_context[:opts]) == :ok
  end
end
