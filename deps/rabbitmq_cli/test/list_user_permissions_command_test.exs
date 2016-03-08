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
  import TestHelper
  import ExUnit.CaptureIO

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :net_kernel.connect_node(context[:target])
    on_exit(context, fn -> :erlang.disconnect_node(context[:target]) end)

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
      opts: %{node: context[:target], timeout: context[:test_timeout]},
      result: default_result,
      no_such_user: no_such_user_result,
      timeout: {:badrpc, :timeout}
    }
  end

## -------------------------------- Usage -------------------------------------

  test "wrong number of arguments results in usage print" do
    assert capture_io(fn ->
      ListUserPermissionsCommand.list_user_permissions([],%{})
    end) =~ ~r/Usage:/

    assert capture_io(fn ->
      ListUserPermissionsCommand.list_user_permissions(["guest", "extra"],%{})
    end) =~ ~r/Usage:/
  end

## ------------------------------- Username -----------------------------------

  @tag target: get_rabbit_hostname, test_timeout: :infinity, username: "guest"
  test "valid user returns a list of permissions", context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [context[:username]], context[:opts]) == context[:result]
  end

  @tag target: get_rabbit_hostname, test_timeout: :infinity, username: "interloper"
  test "invalid user returns a no-such-user error", context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [context[:username]], context[:opts]) == context[:no_such_user]
  end

## --------------------------------- Flags ------------------------------------

  @tag target: :jake@thedog, test_timeout: :infinity, username: "guest"
  test "invalid or inactive RabbitMQ node returns a bad RPC error", context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [context[:username]], context[:opts]) == {:badrpc, :nodedown}
  end

  @tag target: get_rabbit_hostname, test_timeout: 30, username: "guest"
  test "long user-defined timeout doesn't interfere with operation", context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [context[:username]],
      context[:opts]
    ) == context[:result]
  end

  @tag target: get_rabbit_hostname, test_timeout: 0, username: "guest"
  test "timeout causes command to return a bad RPC", context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [context[:username]],
      context[:opts]
    ) == context[:timeout]
  end
end
