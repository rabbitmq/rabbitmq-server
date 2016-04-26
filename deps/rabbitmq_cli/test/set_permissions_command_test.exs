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


defmodule SetPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @vhost "test1"
  @user "guest"
  @root   "/"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    add_vhost @vhost

    on_exit([], fn ->
      delete_vhost @vhost
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do

    on_exit(context, fn ->
      set_permissions context[:user], context[:vhost], [".*", ".*", ".*"]
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname
      }
    }
  end

  test "wrong number of arguments leads to an arg count error" do
    assert SetPermissionsCommand.set_permissions([], %{}) == {:not_enough_args, []}
    assert SetPermissionsCommand.set_permissions(["insufficient"], %{}) == {:not_enough_args, ["insufficient"]}
    assert SetPermissionsCommand.set_permissions(["not", "enough"], %{}) == {:not_enough_args, ["not", "enough"]}
    assert SetPermissionsCommand.set_permissions(["not", "quite", "enough"], %{}) == {:not_enough_args, ["not", "quite", "enough"]}
    assert SetPermissionsCommand.set_permissions(["this", "is", "way", "too", "many"], %{}) == {:too_many_args, ["this", "is", "way", "too", "many"],}
  end

  @tag user: @user, vhost: @vhost
  test "a well-formed, host-specific command returns okay", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})
    assert SetPermissionsCommand.set_permissions(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      vhost_opts
    ) == :ok

    assert List.first(list_permissions(context[:vhost]))[:configure] == "^#{context[:user]}-.*"
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert SetPermissionsCommand.set_permissions([@user, ".*", ".*", ".*"], opts) == {:badrpc, :nodedown}
  end

  @tag user: @user, vhost: @root
  test "a well-formed command with no vhost runs against the default", context do
    assert SetPermissionsCommand.set_permissions(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      context[:opts]
    ) == :ok

    assert List.first(list_permissions(context[:vhost]))[:configure] == "^#{context[:user]}-.*"
  end

  @tag user: "interloper", vhost: @root
  test "an invalid user returns a no-such-user error", context do
    assert SetPermissionsCommand.set_permissions(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      context[:opts]
    ) == {:error, {:no_such_user, context[:user]}}

    assert List.first(list_permissions(context[:vhost]))[:configure] == ".*"
  end

  @tag user: @user, vhost: "wintermute"
  test "an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})
    assert SetPermissionsCommand.set_permissions(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag user: @user, vhost: @root
  test "Invalid regex patterns return error", context do
    assert SetPermissionsCommand.set_permissions(
      [context[:user], "^#{context[:user]}-.*", ".*", "*"],
      context[:opts]
    ) == {:error, {:invalid_regexp, '*', {'nothing to repeat', 0}}}

    # asserts that the bad command didn't change anything
    assert List.first(list_permissions(context[:vhost])) == [user: context[:user], configure: ".*", write: ".*", read: ".*"]
  end
end
