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


defmodule ClearPermissionsTest do
  use ExUnit.Case, async: false
  import TestHelper

  @user     "user1"
  @password "password"
  @default_vhost "/"
  @specific_vhost "vhost1"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)
    add_user(@user, @password)
    add_vhost(@specific_vhost)

    on_exit([], fn ->
      delete_user(@user)
      delete_vhost(@specific_vhost)
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    set_permissions(@user, @default_vhost, ["^#{@user}-.*", ".*", ".*"])
    set_permissions(@user, @specific_vhost, ["^#{@user}-.*", ".*", ".*"])

    {
      :ok,
      opts: %{node: get_rabbit_hostname},
      vhost_options: %{node: get_rabbit_hostname, param: context[:vhost]}
    }
  end

  @tag user: "fake_user"
  test "can't clear permissions for non-existing user", context do
    assert ClearPermissionsCommand.clear_permissions([context[:user]], context[:opts]) == {:error, {:no_such_user, context[:user]}}
  end

  test "invalid arguments return arg count error" do
    assert ClearPermissionsCommand.clear_permissions([], %{}) == {:not_enough_args, []}
    assert ClearPermissionsCommand.clear_permissions(["too", "many"], %{}) == {:too_many_args, ["too", "many"]}
  end

  @tag user: @user
  test "a valid username clears permissions", context do
    assert ClearPermissionsCommand.clear_permissions([context[:user]], context[:opts]) == :ok

    assert list_permissions(@default_vhost)
    |> Enum.filter(fn(record) -> record[:user] == context[:user] end) == []
  end

  test "on an invalid node, return a badrpc message" do
    bad_node = :jake@thedog
    arg = ["some_name"]
    opts = %{node: bad_node}

    assert ClearPermissionsCommand.clear_permissions(arg, opts) == {:badrpc, :nodedown}
  end

  @tag user: @user, vhost: @specific_vhost
  test "on a valid specified vhost, clear permissions", context do
    assert ClearPermissionsCommand.clear_permissions([context[:user]], context[:vhost_options]) == :ok

    assert list_permissions(context[:vhost])
    |> Enum.filter(fn(record) -> record[:user] == context[:user] end) == []
  end

  @tag user: @user, vhost: "bad_vhost"
  test "on an invalid vhost, return no_such_vhost error", context do
    assert ClearPermissionsCommand.clear_permissions([context[:user]], context[:vhost_options]) == {:error, {:no_such_vhost, context[:vhost]}}
  end
end
