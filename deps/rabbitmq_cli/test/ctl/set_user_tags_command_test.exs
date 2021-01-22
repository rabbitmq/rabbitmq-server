## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetUserTagsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetUserTagsCommand

  @user     "user1"
  @password "password"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_user @user, @password

    on_exit([], fn ->
      delete_user(@user)
    end)

    :ok
  end

  setup context do
    context[:user] # silences warnings
    on_exit([], fn -> set_user_tags(context[:user], []) end)

    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: on an incorrect number of arguments, return an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, :imperator], opts))
  end

  @tag user: @user, tags: [:imperator]
  test "run: on a single optional argument, add a flag to the user", context  do
    @command.run(
      [context[:user] | context[:tags]],
      context[:opts]
    )

    result = Enum.find(
      list_users(),
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: "interloper", tags: [:imperator]
  test "run: on an invalid user, get a no such user error", context do
    assert @command.run(
      [context[:user] | context[:tags]],
      context[:opts]
    ) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user, tags: [:imperator, :generalissimo]
  test "run: on multiple optional arguments, add all flags to the user", context  do
    @command.run(
      [context[:user] | context[:tags]],
      context[:opts]
    )

    result = Enum.find(
      list_users(),
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: @user, tags: [:imperator]
  test "run: with no optional arguments, clear user tags", context  do

    set_user_tags(context[:user], context[:tags])

    @command.run([context[:user]], context[:opts])

    result = Enum.find(
      list_users(),
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == []
  end

  @tag user: @user, tags: [:imperator]
  test "run: identical calls are idempotent", context  do

    set_user_tags(context[:user], context[:tags])

    assert @command.run(
      [context[:user] | context[:tags]],
      context[:opts]
    ) == :ok

    result = Enum.find(
      list_users(),
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:tags]
  end

  @tag user: @user, old_tags: [:imperator], new_tags: [:generalissimo]
  test "run: if different tags exist, overwrite them", context  do

    set_user_tags(context[:user], context[:old_tags])

    assert @command.run(
      [context[:user] | context[:new_tags]],
      context[:opts]
    ) == :ok

    result = Enum.find(
      list_users(),
      fn(record) -> record[:user] == context[:user] end
    )

    assert result[:tags] == context[:new_tags]
  end

  @tag user: @user, tags: ["imperator"]
  test "banner", context  do
    assert @command.banner(
        [context[:user] | context[:tags]],
        context[:opts]
      )
      =~ ~r/Setting tags for user \"#{context[:user]}\" to \[#{context[:tags]}\] \.\.\./
  end

end
