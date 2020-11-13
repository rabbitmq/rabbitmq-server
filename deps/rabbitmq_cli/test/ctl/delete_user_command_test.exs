## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule DeleteUserCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.DeleteUserCommand
  @user "username"
  @password "password"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  setup context do
    add_user(context[:user], @password)
    on_exit(context, fn -> delete_user(context[:user]) end)

    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  @tag user: @user
  test "validate: argument count validates" do
    assert @command.validate(["one"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag user: @user
  test "run: A valid username returns ok", context do
    assert @command.run([context[:user]], context[:opts]) == :ok

    assert list_users() |> Enum.count(fn(record) -> record[:user] == context[:user] end) == 0
  end

  test "run: An invalid Rabbit node returns a bad rpc message" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run(["username"], opts))
  end

  @tag user: @user
  test "run: An invalid username returns an error", context do
    assert @command.run(["no_one"], context[:opts]) == {:error, {:no_such_user, "no_one"}}
  end

  @tag user: @user
  test "banner", context do
    s = @command.banner([context[:user]], context[:opts])
    assert s =~ ~r/Deleting user/
    assert s =~ ~r/\"#{context[:user]}\"/
  end
end
