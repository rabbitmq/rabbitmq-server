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

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup default_context do
    :net_kernel.connect_node(get_rabbit_hostname)
    on_exit(default_context, fn -> :erlang.disconnect_node(get_rabbit_hostname) end)

    default_result = [
      [
        {:vhost,<<"/">>},
        {:configure,<<".*">>},
        {:write,<<".*">>},
        {:read,<<".*">>}
      ]
    ]

    no_such_user_result = {:error, {:no_such_user, default_context[:username]}}

    {:ok, result: default_result, no_such_user: no_such_user_result}
  end

  @tag username: "guest"
  test "valid user returns a list of permissions", default_context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [default_context[:username]], []) == default_context[:result]
  end

  @tag username: "interloper"
  test "invalid user returns a no-such-user error", default_context do
    assert ListUserPermissionsCommand.list_user_permissions(
      [default_context[:username]], []) == default_context[:no_such_user]
  end
end
