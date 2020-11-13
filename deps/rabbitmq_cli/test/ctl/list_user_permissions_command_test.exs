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
