
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


defmodule ClearUserLimitsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClearUserLimitsCommand

  @user "someone"
  @password "password"
  @limittype "max-channels"
  @channel_definition "{\"max-channels\":100}"
  @definition "{\"max-channels\":500, \"max-connections\":100}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_user @user, @password

    on_exit([], fn ->
      delete_user @user
    end)

    :ok
  end

  setup context do
    on_exit(context, fn ->
      clear_user_limits(context[:user])
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname()
      }
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

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, @limittype], opts))
  end

  test "run: if limit exists, returns ok and clears it", context do
    :ok = set_user_limits(@user, @channel_definition)

    assert get_user_limits(@user) != []

    assert @command.run(
      [@user, @limittype],
      context[:opts]
    ) == :ok

    assert get_user_limits(@user) == %{}
  end

  test "run: if limit exists, returns ok and clears all limits for the given user", context do
    :ok = set_user_limits(@user, @definition)

    assert get_user_limits(@user) != []

    assert @command.run(
      [@user, "all"],
      context[:opts]
    ) == :ok

    assert get_user_limits(@user) == %{}
  end

  @tag user: "bad-user"
  test "run: a non-existent user returns an error", context do

    assert @command.run(
      [context[:user], @limittype],
      context[:opts]
    ) == {:error, {:no_such_user, "bad-user"}}
  end

  test "banner: for a limit type", context do

    s = @command.banner(
      [@user, @limittype],
      context[:opts]
    )

    assert s == "Clearing \"#{@limittype}\" limit for user \"#{@user}\" ..."
  end

  test "banner: for all", context do

    s = @command.banner(
      [@user, "all"],
      context[:opts]
    )

    assert s == "Clearing all limits for user \"#{@user}\" ..."
  end

end
