## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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

  test "validate: no positional arguments fails" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: too many positional arguments fails" do
    assert @command.validate(["user", "password", "extra"], %{}) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: two arguments passes" do
    assert @command.validate(["user", "password"], %{}) == :ok
  end

  test "validate: one argument passes" do
    assert @command.validate(["user"], %{}) == :ok
  end

  @tag user: "", password: "password"
  test "validate: an empty username fails", context do
    assert match?({:validation_failure, {:bad_argument, _}}, @command.validate([context[:user], context[:password]], context[:opts]))
  end

  # Blank passwords are currently allowed, they make sense
  # e.g. when a user only authenticates using X.509 certificates.
  # Credential validators can be used to require passwords of a certain length
  # or following a certain pattern. This is a core server responsibility. MK.
  @tag user: "some_rando", password: ""
  test "validate: an empty password is allowed", context do
    assert @command.validate([context[:user], context[:password]], context[:opts]) == :ok
  end

  @tag user: "someone", password: "password"
  test "run: request to a non-existent node returns a badrpc", context do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([context[:user], context[:password]], opts))
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
  test "output: formats a user_already_exists error", context do
    {:error, 70, "User \"someone\" already exists"} =
      @command.output({:error, {:user_already_exists, context[:user]}}, %{})
  end
end
