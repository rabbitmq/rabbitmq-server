## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetPermissionsCommand

  @vhost "test1"
  @user "guest"
  @root   "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost @vhost

    on_exit([], fn ->
      delete_vhost @vhost
    end)

    :ok
  end

  setup context do

    on_exit(context, fn ->
      set_permissions context[:user], context[:vhost], [".*", ".*", ".*"]
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        vhost: context[:vhost]
      }
    }
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  test "validate: wrong number of arguments leads to an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not", "enough"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not", "quite", "enough"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this", "is", "way", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag user: @user, vhost: @vhost
  test "run: a well-formed, host-specific command returns okay", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      vhost_opts
    ) == :ok

    u = Enum.find(list_permissions(context[:vhost]), fn(x) -> x[:user] == context[:user] end)
    assert u[:configure] == "^#{context[:user]}-.*"
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, ".*", ".*", ".*"], opts))
  end

  @tag user: "interloper", vhost: @root
  test "run: an invalid user returns a no-such-user error", context do
    assert @command.run(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      context[:opts]
    ) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user, vhost: "wintermute"
  test "run: an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:user], "^#{context[:user]}-.*", ".*", ".*"],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag user: @user, vhost: @root
  test "run: invalid regex patterns returns an error", context do
    assert @command.run(
      [context[:user], "^#{context[:user]}-.*", ".*", "*"],
      context[:opts]
    ) == {:error, {:invalid_regexp, '*', {'nothing to repeat', 0}}}

    # asserts that the failed command didn't change anything
    u = Enum.find(list_permissions(context[:vhost]), fn(x) -> x[:user] == context[:user] end)
    assert u == [user: context[:user], configure: ".*", write: ".*", read: ".*"]
  end

  @tag user: @user, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:user], "^#{context[:user]}-.*", ".*", ".*"], vhost_opts)
      =~ ~r/Setting permissions for user \"#{context[:user]}\" in vhost \"#{context[:vhost]}\" \.\.\./
  end
end
