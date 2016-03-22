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


defmodule SetUserTagsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

  @user     "user1"
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
    add_user @user, @password
    on_exit([], fn -> delete_user context[:user] end)

    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "on an incorrect number of arguments, return bad arg" do
    assert capture_io(fn ->
      SetUserTagsCommand.set_user_tags([], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn ->
      assert SetUserTagsCommand.set_user_tags([], %{}) == {:bad_argument, ["<missing>", "<missing>"]}
    end)
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert SetUserTagsCommand.set_user_tags([@user, "imperator"], opts) == {:badrpc, :nodedown}
  end

  @tag user: @user, tags: ["imperator"]
  test "on a single optional argument, add a flag to the user", context  do
    SetUserTagsCommand.set_user_tags(
      [context[:user] | context[:tags]],
      context[:opts]
    )

    result = Enum.find(
      list_users,
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: "interloper", tags: ["imperator"]
  test "on an invalid user, get a no such user error", context do
    assert SetUserTagsCommand.set_user_tags(
      [context[:user] | context[:tags]],
      context[:opts]
    ) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user, tags: ["imperator", "generalissimo"]
  test "on multiple optional arguments, add all flags to the user", context  do
    SetUserTagsCommand.set_user_tags(
      [context[:user] | context[:tags]],
      context[:opts]
    )

    result = Enum.find(
      list_users,
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: @user, tags: ["imperator"]
  test "with no optional arguments, clear user tags", context  do

    set_user_tags(context[:user], context[:tags])

    SetUserTagsCommand.set_user_tags([context[:user]], context[:opts])

    result = Enum.find(
      list_users,
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == []
  end

  @tag user: @user, tags: ["imperator"]
  test "identical calls are idempotent", context  do

    set_user_tags(context[:user], context[:tags])

    assert SetUserTagsCommand.set_user_tags(
      [context[:user] | context[:tags]],
      context[:opts]
    ) == :ok

    result = Enum.find(
      list_users,
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: @user, old_tags: ["imperator"], new_tags: ["generalissimo"]
  test "if different tags exist, overwrite them", context  do

    set_user_tags(context[:user], context[:old_tags])

    assert SetUserTagsCommand.set_user_tags(
      [context[:user] | context[:new_tags]],
      context[:opts]
    ) == :ok

    result = Enum.find(
      list_users,
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:new_tags]
  end
end
