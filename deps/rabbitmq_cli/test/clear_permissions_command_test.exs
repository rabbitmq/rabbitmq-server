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
  import ExUnit.CaptureIO
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
      vhost_options: %{node: get_rabbit_hostname, vhost: context[:vhost]}
    }
  end

  @tag user: "fake_user"
  test "can't clear permissions for non-existing user", context do
    capture_io(fn ->
      assert ClearPermissionsCommand.run([context[:user]], context[:opts]) == {:error, {:no_such_user, context[:user]}}
    end)
  end

  test "invalid arguments return arg count error" do
    assert ClearPermissionsCommand.run([], %{}) == {:not_enough_args, []}
    assert ClearPermissionsCommand.run(["too", "many"], %{}) == {:too_many_args, ["too", "many"]}
  end

  @tag user: @user
  test "a valid username clears permissions", context do
    capture_io(fn ->
      assert ClearPermissionsCommand.run([context[:user]], context[:opts]) == :ok
    end)

    assert list_permissions(@default_vhost)
    |> Enum.filter(fn(record) -> record[:user] == context[:user] end) == []
  end

  test "on an invalid node, return a badrpc message" do
    bad_node = :jake@thedog
    arg = ["some_name"]
    opts = %{node: bad_node}

    capture_io(fn ->
      assert ClearPermissionsCommand.run(arg, opts) == {:badrpc, :nodedown}
    end)
  end

  @tag user: @user, vhost: @specific_vhost
  test "on a valid specified vhost, clear permissions", context do
    capture_io(fn ->
      assert ClearPermissionsCommand.run([context[:user]], context[:vhost_options]) == :ok
    end)

    assert list_permissions(context[:vhost])
    |> Enum.filter(fn(record) -> record[:user] == context[:user] end) == []
  end

  @tag user: @user, vhost: "bad_vhost"
  test "on an invalid vhost, return no_such_vhost error", context do
    capture_io(fn ->
      assert ClearPermissionsCommand.run([context[:user]], context[:vhost_options]) == {:error, {:no_such_vhost, context[:vhost]}}
    end)
  end

  @tag user: @user, vhost: @specific_vhost
  test "print info message by default", context do
    assert capture_io(fn ->
      ClearPermissionsCommand.run([context[:user]], context[:vhost_options])
    end) =~ ~r/Clearing permissions for user \"#{context[:user]}\" in vhost \"#{context[:vhost]}\" \.\.\./
  end

  @tag user: @user, vhost: @specific_vhost
  test "--quiet flag suppresses info message", context do
    opts = Map.merge(context[:vhost_options], %{quiet: true})

    refute capture_io(fn ->
      ClearPermissionsCommand.run([context[:user]], opts)
    end) =~ ~r/Clearing permissions for user "#{context[:user]}" in vhost "#{context[:vhost]}" \.\.\./
  end

  @tag user: @user
  test "--quiet flag is not overwritten when default flag is used", context do
    opts = Map.merge(context[:opts], %{quiet: true})

    refute capture_io(fn ->
      ClearPermissionsCommand.run([context[:user]], opts)
    end) =~ ~r/Clearing permissions for user "#{context[:user]}" in vhost "#{@default_vhost}" \.\.\./
  end
end
