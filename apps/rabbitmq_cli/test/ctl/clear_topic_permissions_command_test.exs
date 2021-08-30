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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ClearTopicPermissionsTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands. ClearTopicPermissionsCommand
  @user     "user1"
  @password "password"
  @specific_vhost "vhost1"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_user(@user, @password)
    add_vhost(@specific_vhost)

    on_exit([], fn ->
      clear_topic_permissions(@user, @specific_vhost)
      delete_user(@user)
      delete_vhost(@specific_vhost)
    end)

    :ok
  end

  setup context do
    set_topic_permissions(@user, @specific_vhost, "amq.topic", "^a", "^b")
    set_topic_permissions(@user, @specific_vhost, "topic1", "^a", "^b")
    {
      :ok,
      opts: %{node: get_rabbit_hostname(), vhost: context[:vhost]}
    }
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  test "validate: expects username and optional exchange" do
    assert @command.validate(["username"], %{}) == :ok
    assert @command.validate(["username", "exchange"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag user: "fake_user"
  test "run: can't clear topic permissions for non-existing user", context do
    assert @command.run([context[:user]], context[:opts]) == {:error, {:no_such_user, context[:user]}}
  end

  @tag user: @user, vhost: "bad_vhost"
  test "run: on an invalid vhost, return no_such_vhost error", context do
    assert @command.run([context[:user]], context[:opts]) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag user: @user, vhost: @specific_vhost
  test "run: with a valid username clears all permissions for vhost", context do
    assert Enum.count(list_user_topic_permissions(@user)) == 2
    assert @command.run([context[:user]], context[:opts]) == :ok

    assert Enum.count(list_user_topic_permissions(@user)) == 0
  end

  @tag user: @user, vhost: @specific_vhost
  test "run: with a valid username and exchange clears only exchange permissions", context do
    assert Enum.count(list_user_topic_permissions(@user)) == 2
    assert @command.run([context[:user], "amq.topic"], context[:opts]) == :ok

    assert Enum.count(list_user_topic_permissions(@user)) == 1
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    arg = ["some_name"]
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run(arg, opts))
  end

  @tag user: @user, vhost: @specific_vhost
  test "banner with username only", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:user]], vhost_opts)
          =~ ~r/Clearing topic permissions for user \"#{context[:user]}\" in vhost \"#{context[:vhost]}\" \.\.\./
  end

  @tag user: @user, vhost: @specific_vhost
  test "banner with username and exchange name", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:user], "amq.topic"], vhost_opts)
          =~ ~r/Clearing topic permissions on \"amq.topic\" for user \"#{context[:user]}\" in vhost \"#{context[:vhost]}\" \.\.\./
  end
end
