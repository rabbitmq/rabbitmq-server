## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListPermissionsCommand

  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity
  @default_options %{vhost: "/", table_headers: true}

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost @vhost
    set_permissions @user, @vhost, ["^guest-.*", ".*", ".*"]

    on_exit([], fn ->
      delete_vhost @vhost
    end)

    :ok
  end

  setup context do
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout],
        vhost: "/"
      }
    }
  end

  test "merge_defaults adds default options" do
    assert @command.merge_defaults([], %{}) == {[], @default_options}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], @default_options}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default",
                                                                          table_headers: true}}
  end

  test "validate: invalid parameters yield an arg count error" do
    assert @command.validate(["extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: on a bad RabbitMQ node, return a badrpc" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag test_timeout: @default_timeout, vhost: @vhost
  test "run: specifying a vhost returns the targeted vhost permissions", context do
    assert @command.run(
      [],
      Map.merge(context[:opts], %{vhost: @vhost})
    ) == [[user: "guest", configure: "^guest-.*", write: ".*", read: ".*"]]
  end

  @tag test_timeout: 30000
  test "run: sufficiently long timeouts don't interfere with results", context do
    results = @command.run([], context[:opts])
    Enum.all?([[user: "guest", configure: ".*", write: ".*", read: ".*"]], fn(perm) ->
      Enum.find(results, fn(found) -> found == perm end)
    end)
  end

  @tag test_timeout: 0
  test "run: timeout causes command to return a bad RPC", context do
    assert @command.run([], context[:opts]) ==
      {:badrpc, :timeout}
  end

  @tag vhost: @root
  test "banner", context do
    ctx = Map.merge(context[:opts], %{vhost: @vhost})
    assert @command.banner([], ctx )
      =~ ~r/Listing permissions for vhost \"#{Regex.escape(ctx[:vhost])}\" \.\.\./
  end
end
