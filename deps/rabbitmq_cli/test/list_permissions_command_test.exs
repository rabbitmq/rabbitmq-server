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


defmodule ListPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    add_vhost @vhost
    set_permissions @user, @vhost, ["^guest-.*", ".*", ".*"]

    on_exit([], fn ->
      delete_vhost @vhost
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname,
        timeout: context[:test_timeout]
      }
    }
  end

  test "invalid parameters yield bad_argument" do
    assert capture_io(fn ->
      ListPermissionsCommand.list_permissions(["extra"], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn ->
      assert ListPermissionsCommand.list_permissions(["extra"], %{}) == {:bad_argument, ["extra"]}
    end)
  end

  @tag test_timeout: @default_timeout
  test "no options lists permissions on the default", context do
    assert ListPermissionsCommand.list_permissions([], context[:opts]) ==
      [[user: "guest", configure: ".*", write: ".*", read: ".*"]]
  end

  test "on a bad RabbitMQ node, return a badrpc" do
    target = :jake@thedog
    opts = %{node: :jake@thedog, timeout: :infinity}
    :net_kernel.connect_node(target)
    assert ListPermissionsCommand.list_permissions([], opts) == {:badrpc, :nodedown}
  end

  @tag test_timeout: @default_timeout, vhost: @vhost
  test "specifying a vhost returns the targeted vhost permissions", context do
    assert ListPermissionsCommand.list_permissions(
      [],
      Map.merge(context[:opts], %{param: @vhost})
    ) == [[user: "guest", configure: "^guest-.*", write: ".*", read: ".*"]]
  end

  @tag test_timeout: 30
  test "sufficiently long timeouts don't interfere with results", context do
    assert ListPermissionsCommand.list_permissions([], context[:opts]) ==
      [[user: "guest", configure: ".*", write: ".*", read: ".*"]]
  end

  @tag test_timeout: 0
  test "timeout causes command to return a bad RPC", context do
    assert ListPermissionsCommand.list_permissions([], context[:opts]) ==
      {:badrpc, :timeout}
  end
end
