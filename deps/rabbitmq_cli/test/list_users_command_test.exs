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


defmodule ListUsersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @user     "user1"
  @password "password"
  @guest    "guest"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    std_result = [
      [{:user,@guest},{:tags,[:administrator]}],
      [{:user,@user},{:tags,[]}]
    ]

    {:ok, std_result: std_result}
  end

  setup context do
    add_user @user, @password
    on_exit([], fn -> delete_user @user end)

    {:ok, opts: %{node: get_rabbit_hostname, timeout: context[:test_timeout]}}
  end

  test "On incorrect number of commands, return an arg count error" do
    assert ListUsersCommand.list_users(["extra"], %{}) == {:too_many_args, ["extra"]}
  end

  @tag test_timeout: :infinity
  test "On a successful query, return an array of lists of tuples", context do
    matches_found = ListUsersCommand.list_users([], context[:opts])

    assert Enum.all?(matches_found, fn(user) ->
      Enum.find(context[:std_result], fn(found) -> found == user end)
    end)
  end

  test "On an invalid rabbitmq node, return a bad rpc" do
    assert ListUsersCommand.list_users([], %{node: :jake@thedog, timeout: :infinity}) == {:badrpc, :nodedown}
  end

  @tag test_timeout: 30
  test "sufficiently long timeouts don't interfere with results", context do
    # checks to ensure that all expected users are in the results
    assert ListUsersCommand.list_users([], context[:opts])
    |> Enum.all?(fn(user) ->
      Enum.find(context[:std_result], fn(found) -> found == user end)
    end)
  end

  @tag test_timeout: 0
  test "timeout causes command to return a bad RPC", context do
    assert ListUsersCommand.list_users([], context[:opts]) == 
      {:badrpc, :timeout}
  end
end

