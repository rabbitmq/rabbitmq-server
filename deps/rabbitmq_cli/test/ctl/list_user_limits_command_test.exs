## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ListUserLimitsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListUserLimitsCommand

  @user "guest"
  @user1 "test_user1"
  @password1 "password1"
  @connection_limit_defn "{\"max-connections\":100}"
  @channel_limit_defn "{\"max-channels\":1000}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    user = context[:user] || @user

    clear_user_limits(user)

    on_exit(context, fn ->
      clear_user_limits(user)
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        global: true
      },
      user: user
    }
  end

  test "merge_defaults: does not change defined user" do
    assert match?({[], %{user: "test_user"}}, @command.merge_defaults([], %{user: "test_user"}))
  end

  test "validate: providing arguments fails validation" do
    assert @command.validate(["many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: a well-formed command returns an empty list if there are no limits", context do
    assert @command.run([], context[:opts]) == []
  end

  test "run: a well-formed user specific command returns an empty json object if there are no limits" do
    assert @command.run([], %{node: get_rabbit_hostname(),
                              user: @user}) == "{}"
  end

  test "run: list limits for all users", context do
    add_user(@user1, @password1)
    on_exit(fn() ->
      delete_user(@user1)
    end)
    set_user_limits(@user, @connection_limit_defn)
    set_user_limits(@user1, @channel_limit_defn)

    assert Enum.sort(@command.run([], context[:opts])) ==
           Enum.sort([[user: @user,  limits: @connection_limit_defn],
                      [user: @user1, limits: @channel_limit_defn]])
  end

  test "run: list limits for a single user", context do
    user_opts = Map.put(context[:opts], :user, @user)
    set_user_limits(@user, @connection_limit_defn)

    assert @command.run([], user_opts) ==
           [[user: @user, limits: @connection_limit_defn]]
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, user: "guest", timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag user: "user"
  test "run: providing a non-existent user reports an error", _context do
    s = "non-existent-user"

    assert @command.run([], %{node: get_rabbit_hostname(),
                              user: s}) == {:error, {:no_such_user, s}}
  end

  test "banner", context do
    assert @command.banner([], %{user: context[:user]})
      == "Listing limits for user \"#{context[:user]}\" ..."
    assert @command.banner([], %{global: true})
      == "Listing limits for all users ..."
  end
end
