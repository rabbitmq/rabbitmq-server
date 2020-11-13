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
