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

  @vhost "test"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  setup context do
    on_exit(context, fn -> delete_vhost(context[:vhost]) end)
    :ok
  end

  test "wrong number of arguments results in arg count errors" do
    assert AddVhostCommand.add_vhost([], %{}) == {:not_enough_args, []}
    assert AddVhostCommand.add_vhost(["test", "extra"], %{}) == {:too_many_args, ["test", "extra"]}
  end

  @tag vhost: @vhost
  test "A valid name to an active RabbitMQ node is successful", context do
    capture_io(fn ->
      assert AddVhostCommand.add_vhost([context[:vhost]], context[:opts]) == :ok
    end)

    assert list_vhosts |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  @tag vhost: ""
  test "An empty string to an active RabbitMQ node is still successful", context do
    capture_io(fn ->
      assert AddVhostCommand.add_vhost([context[:vhost]], context[:opts]) == :ok
    end)

    assert list_vhosts |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  test "A call to invalid or inactive RabbitMQ node returns a nodedown" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    capture_io(fn ->
      assert AddVhostCommand.add_vhost(["na"], opts) == {:badrpc, :nodedown}
    end)
  end

  test "Adding the same host twice results in a host exists message", context do
    add_vhost context[:vhost]

    capture_io(fn ->
      assert AddVhostCommand.add_vhost([context[:vhost]], context[:opts]) == 
        {:error, {:vhost_already_exists, context[:vhost]}}
    end)

    assert list_vhosts |> Enum.count(fn(record) -> record[:name] == context[:vhost] end) == 1
  end

  @tag vhost: @vhost
  test "print info message by default", context do
    assert capture_io(fn ->
      AddVhostCommand.add_vhost([context[:vhost]], context[:opts])
    end) =~ ~r/Adding vhost \"#{context[:vhost]}\" \.\.\./
  end

  @tag vhost: @vhost
  test "--quiet flag suppresses info message", context do
    opts = Map.merge(context[:opts], %{quiet: true})
    refute capture_io(fn ->
      AddVhostCommand.add_vhost([context[:vhost]], opts)
    end) =~ ~r/Adding vhost \"#{context[:vhost]}\" \.\.\./
  end
end
