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


defmodule DeleteVhostCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
			:net_kernel.stop()
		end)

    :ok
  end

  setup context do
    add_vhost(context[:vhost])
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "wrong number of arguments results in usage print" do
    assert capture_io(fn ->
      DeleteVhostCommand.delete_vhost([], %{})
    end) =~ ~r/Usage:/

		capture_io(fn ->
      assert DeleteVhostCommand.delete_vhost([], %{}) == {:bad_argument, []}
		end)

    assert capture_io(fn ->
      DeleteVhostCommand.delete_vhost(["test", "extra"], %{})
    end) =~ ~r/Usage:/

    capture_io(fn ->
      assert DeleteVhostCommand.delete_vhost(["test", "extra"], %{}) == {:bad_argument, ["extra"]}
    end) =~ ~r/Usage:/
  end

  @tag vhost: "test"
  test "A valid name to an active RabbitMQ node is successful", context do
    assert DeleteVhostCommand.delete_vhost([context[:vhost]], context[:opts]) == :ok
		assert list_vhosts |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 0
  end

  @tag vhost: ""
  test "An empty string to an active RabbitMQ node is successful", context do
    assert DeleteVhostCommand.delete_vhost([context[:vhost]], context[:opts]) == :ok
		assert list_vhosts |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 0
  end

  test "A call to invalid or inactive RabbitMQ node returns a nodedown" do
		target = :jake@thedog
		:net_kernel.connect_node(target)
		opts = %{node: target}

    assert DeleteVhostCommand.delete_vhost(["na"], opts) == {:badrpc, :nodedown}
  end

  @tag vhost: "test"
  test "Deleting the same host twice results in a host not found message", context do
    DeleteVhostCommand.delete_vhost([context[:vhost]], context[:opts])
    assert DeleteVhostCommand.delete_vhost([context[:vhost]], context[:opts]) == 
      {:error, {:no_such_vhost, context[:vhost]}}
  end
end
