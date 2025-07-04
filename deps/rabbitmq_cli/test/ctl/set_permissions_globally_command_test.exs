## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule SetPermissionsGloballyCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetPermissionsGloballyCommand

  @vhost1 "test1"
  @vhost2 "test2"
  @vhost3 "test3"

  @user "guest"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    for v <- [@vhost1, @vhost2, @vhost3], do: add_vhost(v)

    on_exit([], fn ->
      for v <- [@vhost1, @vhost2, @vhost3], do: delete_vhost(v)
    end)

    :ok
  end

  setup context do
    on_exit(context, fn ->
      set_permissions_globally(context[:user], [".*", ".*", ".*"])
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname()
      }
    }
  end

  @tag user: @user
  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  @tag user: @user
  test "validate: wrong number of arguments leads to an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not", "enough"], %{}) == {:validation_failure, :not_enough_args}

    assert @command.validate(["not", "quite", "enough"], %{}) ==
             {:validation_failure, :not_enough_args}

    assert @command.validate(["this", "is", "way", "too", "many"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  @tag user: @user
  test "run: a well-formed, host-specific command returns okay", context do
    assert @command.run(
             [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
             context[:opts]
           ) == :ok

    p1 = Enum.find(list_permissions(@vhost1), fn x -> x[:user] == context[:user] end)
    p2 = Enum.find(list_permissions(@vhost2), fn x -> x[:user] == context[:user] end)
    p3 = Enum.find(list_permissions(@vhost3), fn x -> x[:user] == context[:user] end)

    assert p1[:configure] == "^#{context[:user]}-.*"
    assert p2[:configure] == "^#{context[:user]}-.*"
    assert p3[:configure] == "^#{context[:user]}-.*"
  end

  @tag user: @user
  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, ".*", ".*", ".*"], opts))
  end

  @tag user: "interloper"
  test "run: an invalid user returns a no-such-user error", context do
    assert @command.run(
             [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
             context[:opts]
           ) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user
  test "run: invalid regex patterns returns an error", context do
    p1 = Enum.find(list_permissions(@vhost1), fn x -> x[:user] == context[:user] end)
    p2 = Enum.find(list_permissions(@vhost2), fn x -> x[:user] == context[:user] end)
    p3 = Enum.find(list_permissions(@vhost3), fn x -> x[:user] == context[:user] end)

  assert match?({:error, {:invalid_regexp, ~c"*", _}}, @command.run(
           [context[:user], "^#{context[:user]}-.*", ".*", "*"],
           context[:opts]
         ))

    # asserts that the failed command didn't change anything
    p4 = Enum.find(list_permissions(@vhost1), fn x -> x[:user] == context[:user] end)
    p5 = Enum.find(list_permissions(@vhost2), fn x -> x[:user] == context[:user] end)
    p6 = Enum.find(list_permissions(@vhost3), fn x -> x[:user] == context[:user] end)

    assert p1 == p4
    assert p2 == p5
    assert p3 == p6
  end

  @tag user: @user
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:user], "^#{context[:user]}-.*", ".*", ".*"], vhost_opts) =~
             ~r/Setting permissions for user \"#{context[:user]}\" in all virtual hosts \.\.\./
  end
end
