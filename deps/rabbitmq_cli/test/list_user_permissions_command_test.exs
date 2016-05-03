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


defmodule ListUserPermissionsCommandTest do
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
    default_result = [
      [
        {:vhost,<<"/">>},
        {:configure,<<".*">>},
        {:write,<<".*">>},
        {:read,<<".*">>}
      ]
    ]

    no_such_user_result = {:error, {:no_such_user, context[:username]}}

    {
      :ok,
      opts: %{node: get_rabbit_hostname, timeout: context[:test_timeout]},
      result: default_result,
      no_such_user: no_such_user_result,
      timeout: {:badrpc, :timeout}
    }
  end

## -------------------------------- Usage -------------------------------------

  test "wrong number of arguments results in an arg count error" do
    assert ListUserPermissionsCommand.list_user_permissions([], %{}) == {:not_enough_args, []}
    assert ListUserPermissionsCommand.list_user_permissions(["guest", "extra"], %{}) == {:too_many_args, ["guest", "extra"]}
  end

## ------------------------------- Username -----------------------------------

  @tag test_timeout: :infinity, username: "guest"
  test "valid user returns a list of permissions", context do
    capture_io(fn ->
      assert(ListUserPermissionsCommand.list_user_permissions(
        [context[:username]], context[:opts]) == context[:result])
    end)
  end

  @tag test_timeout: :infinity, username: "interloper"
  test "invalid user returns a no-such-user error", context do
    capture_io(fn ->
      assert ListUserPermissionsCommand.list_user_permissions(
        [context[:username]], context[:opts]) == context[:no_such_user]
    end)
  end

## --------------------------------- Flags ------------------------------------

  test "invalid or inactive RabbitMQ node returns a bad RPC error" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target, timeout: :infinity}

    capture_io(fn ->
      assert ListUserPermissionsCommand.list_user_permissions(["guest"], opts) == {:badrpc, :nodedown}
    end)
  end

  @tag test_timeout: 30, username: "guest"
  test "long user-defined timeout doesn't interfere with operation", context do
    capture_io(fn ->
      assert ListUserPermissionsCommand.list_user_permissions(
        [context[:username]],
        context[:opts]
      ) == context[:result]
    end)
  end

  @tag test_timeout: 0, username: "guest"
  test "timeout causes command to return a bad RPC", context do
    capture_io(fn ->
      assert ListUserPermissionsCommand.list_user_permissions(
        [context[:username]],
        context[:opts]
      ) == context[:timeout]
    end)
  end

  @tag test_timeout: :infinity
  test "print info message by default", context do
    assert capture_io(fn ->
      ListUserPermissionsCommand.list_user_permissions(
        [context[:username]],
        context[:opts]
      )
    end) =~ ~r/Listing permissions for user \"#{context[:username]}\" \.\.\./
  end

  @tag test_timeout: :infinity
  test "--quiet flag suppresses info message", context do
    opts = Map.merge(context[:opts], %{quiet: true})
    refute capture_io(fn ->
      ListUserPermissionsCommand.list_user_permissions(
        [context[:username]],
        opts
      )
    end) =~ ~r/Listing permissions for user \"#{context[:username]}\" \.\.\./
  end
end
