## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListUserTopicPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListUserTopicPermissionsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    set_topic_permissions("guest", "/", "amq.topic", "^a", "^b")
    set_topic_permissions("guest", "/", "topic1", "^a", "^b")

    on_exit([], fn ->
      clear_topic_permissions("guest", "/")
    end)

    :ok
  end

  setup context do
    no_such_user_result = {:error, {:no_such_user, context[:username]}}

    {
      :ok,
      opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout]},
      no_such_user: no_such_user_result,
      timeout: {:badrpc, :timeout}
    }
  end

## -------------------------------- Usage -------------------------------------

  test "validate: expect username argument" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["guest", "extra"], %{}) == {:validation_failure, :too_many_args}
  end

## ------------------------------- Username -----------------------------------

  @tag test_timeout: :infinity, username: "guest"
  test "run: valid user returns a list of topic permissions", context do
    results = @command.run([context[:username]], context[:opts])
    assert Enum.count(results) == 2
  end

  @tag test_timeout: :infinity, username: "interloper"
  test "run: invalid user returns a no-such-user error", context do
    assert @command.run(
      [context[:username]], context[:opts]) == context[:no_such_user]
  end

## --------------------------------- Flags ------------------------------------

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run(["guest"], opts))
  end

  @tag test_timeout: :infinity
  test "banner", context do
    assert @command.banner( [context[:username]], context[:opts])
      =~ ~r/Listing topic permissions for user \"#{context[:username]}\" \.\.\./
  end
end
