
## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetUserLimitsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetUserLimitsCommand

  @user "someone"
  @password "password"
  @conn_definition "{\"max-connections\":100}"
  @channel_definition "{\"max-channels\":200}"

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

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, @conn_definition], opts))
  end

  @tag user: "bad-user"
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
        ["bad_value"]],
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

defp assert_limits(context, definition) do
    limits = get_user_limits(context[:user])
    assert {:ok, limits} == JSON.decode(definition)
  end
end
