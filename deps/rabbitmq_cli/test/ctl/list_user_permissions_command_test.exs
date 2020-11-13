## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListUserPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListUserPermissionsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    default_result = [
      [
        {:vhost,<<"/">>},
        {:configure,<<".*">>},
        {:write,<<".*">>},
        {:read,<<".*">>}
      ]
    ]

    no_such_user_result = {:error, {:no_such_user, context[:username]}}

    {
      :ok,
      opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout]},
      result: default_result,
      no_such_user: no_such_user_result,
      timeout: {:badrpc, :timeout}
    }
  end

## -------------------------------- Usage -------------------------------------

  test "validate: wrong number of arguments results in an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["guest", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

## ------------------------------- Username -----------------------------------

  @tag test_timeout: :infinity, username: "guest"
  test "run: valid user returns a list of permissions", context do
    results = @command.run([context[:username]], context[:opts])
    assert Enum.all?(context[:result], fn(perm) ->
      Enum.find(results, fn(found) -> found == perm end)
    end)
  end

  @tag test_timeout: :infinity, username: "interloper"
  test "run: invalid user returns a no-such-user error", context do
    assert @command.run(
      [context[:username]], context[:opts]) == context[:no_such_user]
  end

## --------------------------------- Flags ------------------------------------

  test "run: unreachable RabbitMQ node returns a badrpc" do
    assert match?({:badrpc, _}, @command.run(["guest"], %{node: :jake@thedog, timeout: 200}))
  end

  @tag test_timeout: 30000, username: "guest"
  test "run: long user-defined timeout doesn't interfere with operation", context do
    results = @command.run([context[:username]], context[:opts])
    Enum.all?(context[:result], fn(perm) ->
      Enum.find(results, fn(found) -> found == perm end)
    end)
  end

  @tag test_timeout: 0, username: "guest"
  test "run: timeout causes command to return a bad RPC", context do
    assert @command.run(
      [context[:username]],
      context[:opts]
    ) == context[:timeout]
  end

  @tag test_timeout: :infinity
  test "banner", context do
    assert @command.banner( [context[:username]], context[:opts])
      =~ ~r/Listing permissions for user \"#{context[:username]}\" \.\.\./
  end
end
