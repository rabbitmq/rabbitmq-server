## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListUsersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListUsersCommand

  @user     "user1"
  @password "password"
  @guest    "guest"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    std_result = [
      [{:user,@guest},{:tags,[:administrator]}],
      [{:user,@user},{:tags,[]}]
    ]

    {:ok, std_result: std_result}
  end

  setup context do
    add_user @user, @password
    on_exit([], fn -> delete_user @user end)

    {:ok, opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout]}}
  end

  test "validate: On incorrect number of commands, return an arg count error" do
    assert @command.validate(["extra"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 15000
  test "run: On a successful query, return an array of lists of tuples", context do
    matches_found = @command.run([], context[:opts])

    assert Enum.all?(context[:std_result], fn(user) ->
      Enum.find(matches_found, fn(found) -> found == user end)
    end)
  end

  test "run: On an invalid rabbitmq node, return a bad rpc" do
    assert match?({:badrpc, _}, @command.run([], %{node: :jake@thedog, timeout: 200}))
  end

  @tag test_timeout: 30000
  test "run: sufficiently long timeouts don't interfere with results", context do
    # checks to ensure that all expected users are in the results
    matches_found = @command.run([], context[:opts])

    assert Enum.all?(context[:std_result], fn(user) ->
      Enum.find(matches_found, fn(found) -> found == user end)
    end)
  end

  @tag test_timeout: 0
  test "run: timeout causes command to return a bad RPC", context do
    assert @command.run([], context[:opts]) ==
      {:badrpc, :timeout}
  end

  @tag test_timeout: :infinity
  test "banner", context do
    assert @command.banner([], context[:opts])
      =~ ~r/Listing users \.\.\./
  end
end
