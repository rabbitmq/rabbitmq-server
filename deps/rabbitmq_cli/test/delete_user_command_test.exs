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


defmodule DeleteUserCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @password "password"

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
    add_user(context[:user], @password)
    on_exit(context, fn -> delete_user(context[:user]) end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  @tag user: "username"
  test "The wrong number of arguments returns arg count error" do
    assert DeleteUserCommand.delete_user([], %{}) == {:not_enough_args, []}
    assert DeleteUserCommand.delete_user(["too", "many"], %{}) == {:too_many_args, ["too", "many"]}
  end

  @tag user: "username"
  test "A valid username returns ok", context do
    assert DeleteUserCommand.delete_user([context[:user]], context[:opts]) == :ok
    assert list_users |> Enum.count(fn(record) -> record[:user] == context[:user] end) == 0
  end

  test "An invalid Rabbit node returns a bad rpc message" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert DeleteUserCommand.delete_user(["username"], opts) == {:badrpc, :nodedown}
  end

  @tag user: "username"
  test "An invalid username returns an error", context do
    assert DeleteUserCommand.delete_user(["no_one"], context[:opts]) == {:error, {:no_such_user, "no_one"}}
  end
end
