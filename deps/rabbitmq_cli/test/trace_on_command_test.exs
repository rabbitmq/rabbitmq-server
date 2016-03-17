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

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)
    add_vhost("test")

    on_exit([], fn ->
      delete_vhost("test")
      :erlang.disconnect_node(get_rabbit_hostname)
			:net_kernel.stop()
		end)

    :ok
  end

  setup default_context do
    on_exit(default_context, fn -> trace_off("/") end)
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  setup vhost_context do
    on_exit(vhost_context, fn -> trace_off(vhost_context[:vhost]) end)
    {:ok, opts: %{node: get_rabbit_hostname, param: vhost_context[:vhost]}}
  end

  test "wrong number of arguments triggers usage" do
    assert capture_io(fn ->
      TraceOnCommand.trace_on(["extra"], %{})
    end) =~ ~r/Usage:/
  end
  
  test "on an active node, trace_on command works on default", default_context do
    assert TraceOnCommand.trace_on([], default_context[:opts]) == :ok
  end
  
  test "calls to trace_on are idempotent", default_context do
    TraceOnCommand.trace_on([], default_context[:opts])
    assert TraceOnCommand.trace_on([], default_context[:opts]) == :ok
  end

  test "on an invalid RabbitMQ node, return a nodedown" do
		target = :jake@thedog
		:net_kernel.connect_node(target)
		opts = %{node: target}

    assert TraceOnCommand.trace_on([], opts) == {:badrpc, :nodedown}
  end

  @tag vhost: "test"
  test "on an active node, trace_on command works on named vhost", vhost_context do
    assert TraceOnCommand.trace_on([], vhost_context[:opts]) == :ok
  end
end
