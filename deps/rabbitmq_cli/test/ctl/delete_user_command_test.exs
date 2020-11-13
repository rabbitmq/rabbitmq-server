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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


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
