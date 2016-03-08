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


defmodule AddVhostCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :net_kernel.connect_node(context[:target])
    on_exit(context, fn ->
      delete_vhost(context[:vhost])
      :erlang.disconnect_node(context[:target])
    end)
    {:ok, opts: %{node: context[:target]}}
  end

  test "wrong number of arguments results in usage print" do
    assert capture_io(fn ->
      IO.puts AddVhostCommand.add_vhost([], %{})
    end) =~ ~r/Usage:/

    assert capture_io(fn ->
      AddVhostCommand.add_vhost(["test", "extra"], %{})
    end) =~ ~r/Usage:/
  end

  @tag target: get_rabbit_hostname, vhost: "test"
  test "A valid name to an active RabbitMQ node is successful", context do
    assert AddVhostCommand.add_vhost([context[:vhost]], context[:opts]) == :ok
  end

  @tag target: :jake@thedog
  test "A call to invalid or inactive RabbitMQ node returns a nodedown", context do
    assert AddVhostCommand.add_vhost(["na"], context[:opts]) == {:badrpc, :nodedown}
  end

  @tag target: get_rabbit_hostname, vhost: "test"
  test "Adding the same host twice results in a host exists message", context do
    AddVhostCommand.add_vhost([context[:vhost]], context[:opts])
    assert AddVhostCommand.add_vhost([context[:vhost]], context[:opts]) == 
      {:error, {:vhost_already_exists, context[:vhost]}}
  end
end
