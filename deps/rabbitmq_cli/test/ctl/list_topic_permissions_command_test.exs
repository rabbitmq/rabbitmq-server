## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListTopicPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListTopicPermissionsCommand

  @vhost "test1"
  @user "user1"
  @password "password"
  @root   "/"
  @default_timeout :infinity
  @default_options %{vhost: "/", table_headers: true}

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost(@vhost)
    add_user(@user, @password)
    set_topic_permissions(@user, @vhost, "amq.topic", "^a", "^b")
    set_topic_permissions(@user, @vhost, "topic1", "^a", "^b")

    on_exit([], fn ->
      clear_topic_permissions(@user, @vhost)
      delete_user(@user)
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

  test "merge_defaults adds default vhost" do
    assert @command.merge_defaults([], %{}) == {[], @default_options}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], @default_options}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default",
                                                                          table_headers: true}}
  end

  test "validate: does not expect any parameter" do
    assert @command.validate(["extra"], %{}) == {:validation_failure, :too_many_args}
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag test_timeout: @default_timeout, vhost: @vhost
  test "run: specifying a vhost returns the topic permissions for the targeted vhost", context do
    permissions = @command.run([], Map.merge(context[:opts], %{vhost: @vhost}))
    assert Enum.count(permissions) == 2
    assert Enum.sort(permissions) == [
        [user: @user, exchange: "amq.topic", write: "^a", read: "^b"],
        [user: @user, exchange: "topic1", write: "^a", read: "^b"]
    ]
  end

  @tag vhost: @root
  test "banner", context do
    ctx = Map.merge(context[:opts], %{vhost: @vhost})
    assert @command.banner([], ctx )
      =~ ~r/Listing topic permissions for vhost \"#{Regex.escape(ctx[:vhost])}\" \.\.\./
  end
end
