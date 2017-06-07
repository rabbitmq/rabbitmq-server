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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule AddUserCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AddUserCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    on_exit(context, fn -> delete_user(context[:user]) end)
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: on an inappropriate number of arguments, validate should return an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["one", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag user: "", password: "password"
  test "validate: an empty username triggers usage message", context do
    assert match?({:validation_failure, {:bad_argument, _}}, @command.validate([context[:user], context[:password]], context[:opts]))
  end

  @tag user: "some_rando", password: ""
  test "validate: an empty password is allowed", context do
    assert @command.validate([context[:user], context[:password]], context[:opts]) == :ok
  end

  @tag user: "someone", password: "password"
  test "run: request to a non-existent node returns nodedown", context do
    target = :jake@thedog

    opts = %{node: target}
    assert match?({:badrpc, :nodedown}, @command.run([context[:user], context[:password]], opts))
  end

  @tag user: "someone", password: "password"
  test "run: default case completes successfully", context do
    assert @command.run([context[:user], context[:password]], context[:opts]) == :ok
    assert list_users() |> Enum.count(fn(record) -> record[:user] == context[:user] end) == 1
  end

  @tag user: "someone", password: "password"
  test "run: adding an existing user returns an error", context do
    add_user(context[:user], context[:password])
    assert @command.run([context[:user], context[:password]], context[:opts]) == {:error, {:user_already_exists, context[:user]}}
    assert list_users() |> Enum.count(fn(record) -> record[:user] == context[:user] end) == 1
  end

  @tag user: "someone", password: "password"
  test "banner", context do
    assert @command.banner([context[:user], context[:password]], context[:opts])
      =~ ~r/Adding user \"#{context[:user]}\" \.\.\./
  end

  @tag user: "someone"
  test "output: formats a user_alredy_exists error", context do
    {:error, 70, "User \"someone\" already exists"} =
      @command.output({:error, {:user_already_exists, context[:user]}}, %{})
  end
end
