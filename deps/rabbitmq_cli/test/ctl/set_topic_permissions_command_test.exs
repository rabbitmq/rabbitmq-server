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


defmodule SetTopicPermissionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetTopicPermissionsCommand

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
      clear_topic_permissions context[:user], context[:vhost]
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

  test "validate: expects username, exchange, and pattern arguments" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not", "enough"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["still", "not", "enough"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this", "is", "way", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag user: @user, vhost: @vhost
  test "run: a well-formed, host-specific command returns okay", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:user], "amq.topic", "^a", "^b"],
      vhost_opts
    ) == :ok

    assert List.first(list_user_topic_permissions(context[:user]))[:write] == "^a"
    assert List.first(list_user_topic_permissions(context[:user]))[:read] == "^b"
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@user, "amq.topic", "^a", "^b"], opts))
  end

  @tag user: "interloper", vhost: @root
  test "run: an invalid user returns a no-such-user error", context do
    assert @command.run(
      [context[:user], "amq.topic", "^a", "^b"],
      context[:opts]
    ) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user, vhost: "wintermute"
  test "run: an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:user], "amq.topic", "^a", "^b"],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}

    assert Enum.count(list_user_topic_permissions(context[:user])) == 0
  end

  @tag user: @user, vhost: @root
  test "run: invalid regex patterns return error", context do
    n = Enum.count(list_user_topic_permissions(context[:user]))
    {:error, {:invalid_regexp, _, _}} = @command.run(
                                            [context[:user], "amq.topic", "[", "^b"],
                                            context[:opts]
                                          )
    assert Enum.count(list_user_topic_permissions(context[:user])) == n
  end

  @tag user: @user, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:user], "amq.topic", "^a", "^b"], vhost_opts)
      =~ ~r/Setting topic permissions on \"amq.topic\" for user \"#{context[:user]}\" in vhost \"#{context[:vhost]}\" \.\.\./
  end
end
