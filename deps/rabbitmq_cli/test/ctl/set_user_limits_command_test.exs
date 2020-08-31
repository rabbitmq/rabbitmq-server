## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule SetUserLimitsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetUserLimitsCommand

  @user "someone"
  @password "password"
  @conn_definition "{\"max-connections\":100}"
  @channel_definition "{\"max-channels\":200}"
  @definition "{\"max-connections\":50, \"max-channels\":500}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_user @user, @password

    on_exit([], fn ->
      delete_user @user
    end)

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
        node: get_rabbit_hostname()
      },
      user: user
    }
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not-enough"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: a well-formed, host-specific command returns okay", context do
    assert @command.run(
      [context[:user],
      @conn_definition],
      context[:opts]
    ) == :ok

    assert_limits(context, @conn_definition)
    clear_user_limits(context[:user])

    assert @command.run(
      [context[:user],
      @channel_definition],
      context[:opts]
    ) == :ok

    assert_limits(context, @channel_definition)
  end

  test "run: a well-formed command to set both max-connections and max-channels returns okay", context do
    assert @command.run(
      [context[:user],
      @definition],
      context[:opts]
    ) == :ok

    assert_limits(context, @definition)
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, @conn_definition], opts))
  end

  @tag user: "non-existent-user"
  test "run: providing a non-existent user reports an error", context do

    assert @command.run(
      [context[:user],
      @conn_definition],
      context[:opts]
    ) == {:error, {:no_such_user, context[:user]}}
  end

  test "run: an invalid definition returns a JSON decoding error", context do
    assert match?({:error_string, _},
      @command.run(
        [context[:user],
        ["this_is_not_json"]],
        context[:opts]))

    assert get_user_limits(context[:user]) == %{}
  end

  test "run: invalid limit returns an error", context do
    assert @command.run(
      [context[:user],
      "{\"foo\":\"bar\"}"],
      context[:opts]
    ) == {:error_string, 'Unrecognised terms [{<<"foo">>,<<"bar">>}] in user-limits'}

    assert get_user_limits(context[:user]) == %{}
  end

  test "banner", context do
    assert @command.banner([context[:user], context[:conn_definition]], context[:opts])
      == "Setting user limits to \"#{context[:conn_definition]}\" for user \"#{context[:user]}\" ..."
  end

  #
  # Implementation
  #

  defp assert_limits(context, definition) do
    limits = get_user_limits(context[:user])
    assert {:ok, limits} == JSON.decode(definition)
  end
end
